/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.annotation.InternalApi
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.RecencyList

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object StartingFromSnapshotStage {
  private case class SnapshotState(seqNr: Long, emitted: Boolean)
}

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class StartingFromSnapshotStage[Event](
    cacheCapacity: Int,
    sequenceNumberOfSnapshot: String => Future[Option[Long]],
    loadSnapshot: String => Future[Option[SnapshotDao.SerializedSnapshotRow]],
    createEnvelope: (SerializedSnapshotRow, TimestampOffset) => EventEnvelope[Event])
    extends GraphStage[FlowShape[EventEnvelope[Event], EventEnvelope[Event]]] {
  import StartingFromSnapshotStage._

  val in: Inlet[EventEnvelope[Event]] = Inlet("in")
  val out: Outlet[EventEnvelope[Event]] = Outlet("out")

  override val shape: FlowShape[EventEnvelope[Event], EventEnvelope[Event]] =
    FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler { self =>
      private implicit def ec: ExecutionContext = materializer.executionContext

      private var snapshotState = Map.empty[String, SnapshotState]
      private val recency = RecencyList.emptyWithNanoClock[String]
      private var inProgress = false

      private def updateState(persistenceId: String, seqNr: Long, emitted: Boolean): Unit = {
        snapshotState = snapshotState.updated(persistenceId, SnapshotState(seqNr, emitted))
        recency.update(persistenceId)
        if (recency.size > cacheCapacity)
          recency.removeLeastRecent().foreach { pid =>
            snapshotState -= pid
          }
      }

      private val seqNrOfSnapshotCallback = getAsyncCallback[Try[(EventEnvelope[Event], Option[Long])]] {
        case Success((env, Some(seqNr))) =>
          inProgress = false
          if (env.sequenceNr == seqNr) {
            // snapshot should be emitted, load full snapshot
            loadCorrespondingSnapshot(env)
          } else if (env.sequenceNr > seqNr) {
            // event is ahead of snapshot, emit event
            updateState(env.persistenceId, seqNr, emitted = false)
            push(out, env)
          } else {
            // snapshot will be emitted later, ignore event
            updateState(env.persistenceId, seqNr, emitted = false)
            tryPullOrComplete()
          }

        case Success((env, None)) =>
          inProgress = false
          // no snapshot, emit event
          updateState(env.persistenceId, 0L, emitted = true)
          push(out, env)

        case Failure(exc) =>
          inProgress = false
          failStage(exc)
      }

      private val loadSnapshotCallback = getAsyncCallback[Try[(EventEnvelope[Event], Option[SerializedSnapshotRow])]] {
        case Success((env, Some(snap))) =>
          inProgress = false
          if (env.sequenceNr == snap.seqNr) {
            push(out, createEnvelope(snap, env.offset.asInstanceOf[TimestampOffset]))
            updateState(snap.persistenceId, snap.seqNr, emitted = true)
          } else if (env.sequenceNr > snap.seqNr) {
            // event is ahead of snapshot, emit event
            updateState(snap.persistenceId, snap.seqNr, emitted = false)
            push(out, env)
          } else {
            // snapshot will be emitted later, ignore event
            updateState(snap.persistenceId, snap.seqNr, emitted = false)
            tryPullOrComplete()
          }

        case Success((env, None)) =>
          inProgress = false
          // no snapshot, emit event
          updateState(env.persistenceId, 0L, emitted = true)
          push(out, env)

        case Failure(exc) =>
          inProgress = false
          failStage(exc)
      }

      override def onPush(): Unit = {
        val env = grab(in)
        snapshotState.get(env.persistenceId) match {
          case Some(s) =>
            val eventIsAfterSnapshot = env.sequenceNr > s.seqNr

            if (eventIsAfterSnapshot) {
              // event is after snapshot, emit event
              // we can't ignore it when snapshot is not emitted, because it might have been emitted in
              // previous incarnation, but then the stream was restarted
              push(out, env)
            } else if (!s.emitted && env.sequenceNr == s.seqNr) {
              // trigger emit of snapshot
              loadCorrespondingSnapshot(env)
            } else {
              // event is before (or same as) snapshot, ignore
              tryPullOrComplete()
            }

          case None =>
            seqNrOfCorrespondingSnapshot(env)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (!inProgress)
          super.onUpstreamFinish()
      }

      private def tryPullOrComplete(): Unit = {
        if (isClosed(in))
          completeStage()
        else
          pull(in)
      }

      private def seqNrOfCorrespondingSnapshot(env: EventEnvelope[Event]): Unit = {
        inProgress = true
        sequenceNumberOfSnapshot(env.persistenceId)
          .map(result => (env, result))(ExecutionContext.parasitic)
          .onComplete(seqNrOfSnapshotCallback.invoke)
      }

      private def loadCorrespondingSnapshot(env: EventEnvelope[Event]): Unit = {
        inProgress = true
        loadSnapshot(env.persistenceId)
          .map(result => (env, result))(ExecutionContext.parasitic)
          .onComplete(loadSnapshotCallback.invoke)
      }

      override def onPull(): Unit = {
        tryPullOrComplete()
      }

      setHandler(in, this)
      setHandler(out, this)

    }

}
