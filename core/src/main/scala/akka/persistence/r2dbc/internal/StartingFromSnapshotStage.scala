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
    loadSnapshot: String => Future[Option[SnapshotDao.SerializedSnapshotRow]],
    createEnvelope: SerializedSnapshotRow => EventEnvelope[Event])
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

      private val loadSnapshotCallback = getAsyncCallback[Try[(EventEnvelope[Event], Option[SerializedSnapshotRow])]] {
        case Success((env, Some(snap))) =>
          if (env.sequenceNr == snap.seqNr) {
            push(out, createEnvelope(snap))
            snapshotState = snapshotState.updated(snap.persistenceId, SnapshotState(snap.seqNr, emitted = true))
          } else if (env.sequenceNr > snap.seqNr) {
            // event is ahead of snapshot, emit event
            snapshotState = snapshotState.updated(snap.persistenceId, SnapshotState(snap.seqNr, emitted = false))
            push(out, env)
          } else {
            // snapshot will be emitted later, ignore event
            snapshotState = snapshotState.updated(snap.persistenceId, SnapshotState(snap.seqNr, emitted = false))
            pull(in)
          }
        case Success((env, None)) =>
          // no snapshot, emit event
          snapshotState = snapshotState.updated(env.persistenceId, SnapshotState(0L, emitted = true))
          push(out, env)

        case Failure(exc) =>
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
              pull(in)
            }

          case None =>
            loadCorrespondingSnapshot(env)
        }
      }

      private def loadCorrespondingSnapshot(env: EventEnvelope[Event]): Unit = {
        loadSnapshot(env.persistenceId)
          .map(result => (env, result))(ExecutionContext.parasitic)
          .onComplete(loadSnapshotCallback.invoke)
      }

      override def onPull(): Unit = {
        pull(in)
      }

      setHandler(in, this)
      setHandler(out, this)

    }

}
