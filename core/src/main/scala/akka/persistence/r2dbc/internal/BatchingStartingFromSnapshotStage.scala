/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
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
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import akka.util.RecencyList

/**
 * Alternative to `StartingFromSnapshotStage` for workloads where a projection partition handles many more distinct
 * persistence ids than `cache-capacity`. There, the original stage's cache-miss rate approaches 1, and it becomes fully
 * serialized and round-trip-latency bound: it never pulls the next event until the current lookup completes.
 *
 * This stage resolves cache misses in batches (`SnapshotDao.sequenceNumbersOfSnapshots`) instead of one round-trip per
 * event, and keeps pulling upstream while a batch is in flight. All envelopes pass through a single ordered queue and
 * are only ever decided from its head, so a cache hit can never be emitted ahead of an earlier, still unresolved
 * envelope.
 *
 * A persistence id's cache entry is pinned against LRU eviction for as long as an envelope that depends on it, has not
 * yet been decided, including while its snapshot payload is being loaded. See `loadCorrespondingSnapshot` for why the
 * payload-load case needs the same protection.
 *
 * Loading the snapshot payload itself stays serialized. That happens once per entity, not once per cache eviction, so
 * it is not the dominant cost.
 */
@InternalApi private[r2dbc] object BatchingStartingFromSnapshotStage {
  private case class SnapshotState(seqNr: Long, emitted: Boolean)
  private case object BatchLingerTimerKey
}

/**
 * See class doc.
 */
@InternalApi private[r2dbc] class BatchingStartingFromSnapshotStage[Event](
    cacheCapacity: Int,
    lookupBatchSize: Int,
    maxBufferedEnvelopes: Int,
    batchLinger: FiniteDuration,
    sequenceNumbersOfSnapshots: Set[String] => Future[Map[String, Long]],
    loadSnapshot: String => Future[Option[SnapshotDao.SerializedSnapshotRow]],
    createEnvelope: (SerializedSnapshotRow, TimestampOffset) => EventEnvelope[Event],
    heartbeatAfter: Int,
    createHeartbeat: Instant => EventEnvelope[Event])
    extends GraphStage[FlowShape[EventEnvelope[Event], EventEnvelope[Event]]] {
  import BatchingStartingFromSnapshotStage._

  require(lookupBatchSize > 0, "lookupBatchSize must be > 0")
  require(maxBufferedEnvelopes >= lookupBatchSize, "maxBufferedEnvelopes must be >= lookupBatchSize")

  val in: Inlet[EventEnvelope[Event]] = Inlet("in")
  val out: Outlet[EventEnvelope[Event]] = Outlet("out")

  override val shape: FlowShape[EventEnvelope[Event], EventEnvelope[Event]] =
    FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler { self =>
      private implicit def ec: ExecutionContext = materializer.executionContext

      private var snapshotState = Map.empty[String, SnapshotState]
      private val recency = RecencyList.emptyWithNanoClock[String]

      // ALL not-yet-decided envelopes, in arrival order. Only ever advanced from the head, so that a cache
      // hit can never jump ahead of an earlier envelope that is still waiting on a lookup or a snapshot load.
      private val pendingQueue = mutable.Queue.empty[EventEnvelope[Event]]
      // reference count, per persistence id, of envelopes currently in pendingQueue; used to pin cache entries
      // that a not-yet-dequeued envelope depends on so eviction cannot invalidate a resolution before it is consumed
      private val pinCount = mutable.Map.empty[String, Int]
      // ids seen in pendingQueue that are not cached yet and are not already covered by an in-flight batch
      private val pendingLookupIds = mutable.LinkedHashSet.empty[String]
      // ids covered by the batch currently in flight, so a re-arriving envelope for the same id does not trigger
      // a redundant duplicate lookup
      private val idsInFlight = mutable.Set.empty[String]
      private var batchInFlight = false

      // envelopes that have been decided (should be pushed) but are waiting for downstream demand
      private val readyQueue = mutable.Queue.empty[EventEnvelope[Event]]

      private var awaitingSnapshotLoad = false
      private var upstreamFinished = false

      // for emitting heartbeat events
      private var filterCount = 0L
      private var latestTimestamp = Instant.EPOCH

      private def pin(persistenceId: String): Unit =
        pinCount.update(persistenceId, pinCount.getOrElse(persistenceId, 0) + 1)

      private def unpin(persistenceId: String): Unit =
        pinCount.get(persistenceId) match {
          case Some(1) => pinCount -= persistenceId
          case Some(n) => pinCount.update(persistenceId, n - 1)
          case None    => ()
        }

      private def isPinned(persistenceId: String): Boolean = pinCount.contains(persistenceId)

      private def updateState(persistenceId: String, seqNr: Long, emitted: Boolean): Unit = {
        snapshotState = snapshotState.updated(persistenceId, SnapshotState(seqNr, emitted))
        recency.update(persistenceId)
        var continueEviction = recency.size > cacheCapacity
        while (continueEviction) {
          recency.leastToMostRecent.find(pid => !isPinned(pid)) match {
            case Some(pid) =>
              recency.remove(pid)
              snapshotState -= pid
              continueEviction = recency.size > cacheCapacity
            case None =>
              // every cached entry is pinned; stay temporarily over cacheCapacity rather than evict one in use
              continueEviction = false
          }
        }
      }

      private def emit(env: EventEnvelope[Event]): Unit = {
        filterCount = 0L
        if (isAvailable(out)) push(out, env)
        else readyQueue.enqueue(env)
      }

      private def ignoreOne(env: EventEnvelope[Event]): Unit = {
        val timestamp = env.offset.asInstanceOf[TimestampOffset].timestamp
        if (timestamp.isAfter(latestTimestamp))
          latestTimestamp = timestamp
        filterCount += 1
        if (filterCount >= heartbeatAfter)
          emit(createHeartbeat(latestTimestamp))
      }

      // decision for one envelope whose persistence id is already resolved; only ever called for the
      // current head of pendingQueue (already dequeued by the caller)
      private def handleResolved(env: EventEnvelope[Event], s: SnapshotState): Unit = {
        val eventIsAfterSnapshot = env.sequenceNr > s.seqNr
        if (eventIsAfterSnapshot) {
          emit(env)
        } else if (!s.emitted && env.sequenceNr == s.seqNr) {
          loadCorrespondingSnapshot(env)
        } else {
          ignoreOne(env)
        }
      }

      private val loadSnapshotCallback = getAsyncCallback[Try[(EventEnvelope[Event], Option[SerializedSnapshotRow])]] {
        case Success((env, Some(snap))) =>
          awaitingSnapshotLoad = false
          unpin(env.persistenceId)
          if (env.sequenceNr == snap.seqNr) {
            updateState(snap.persistenceId, snap.seqNr, emitted = true)
            emit(createEnvelope(snap, env.offset.asInstanceOf[TimestampOffset]))
          } else if (env.sequenceNr > snap.seqNr) {
            updateState(snap.persistenceId, snap.seqNr, emitted = false)
            emit(env)
          } else {
            updateState(snap.persistenceId, snap.seqNr, emitted = false)
            ignoreOne(env)
          }
          advance()
          pullIfRoom()
          completeIfDone()

        case Success((env, None)) =>
          awaitingSnapshotLoad = false
          unpin(env.persistenceId)
          updateState(env.persistenceId, 0L, emitted = true)
          emit(env)
          advance()
          pullIfRoom()
          completeIfDone()

        case Failure(exc) =>
          failStage(exc)
      }

      private def loadCorrespondingSnapshot(env: EventEnvelope[Event]): Unit = {
        awaitingSnapshotLoad = true
        // re-pin: advance() already unpinned this id when it dequeued the envelope, but a concurrent batch for
        // another id could otherwise evict it mid-load and race a redundant lookup against this load
        pin(env.persistenceId)
        loadSnapshot(env.persistenceId)
          .map(result => (env, result))(ExecutionContext.parasitic)
          .onComplete(loadSnapshotCallback.invoke)
      }

      private val batchCallback = getAsyncCallback[Try[(Set[String], Map[String, Long])]] {
        case Success((ids, results)) =>
          batchInFlight = false
          idsInFlight --= ids
          ids.foreach { pid =>
            results.get(pid) match {
              case Some(seqNr) => updateState(pid, seqNr, emitted = false)
              case None        => updateState(pid, 0L, emitted = true) // no snapshot for this id
            }
          }
          advance()
          maybeTriggerBatch()
          pullIfRoom()
          completeIfDone()

        case Failure(exc) =>
          failStage(exc)
      }

      private def triggerBatch(): Unit = {
        cancelTimer(BatchLingerTimerKey)
        if (pendingLookupIds.nonEmpty && !batchInFlight) {
          val ids = pendingLookupIds.toSet
          pendingLookupIds.clear()
          idsInFlight ++= ids
          batchInFlight = true
          sequenceNumbersOfSnapshots(ids)
            .map(results => (ids, results))(ExecutionContext.parasitic)
            .onComplete(batchCallback.invoke)
        }
      }

      private def maybeTriggerBatch(): Unit = {
        if (!batchInFlight && pendingLookupIds.nonEmpty) {
          // once upstream is finished no further ids can arrive, so there is nothing left to linger for
          if (upstreamFinished || pendingLookupIds.size >= lookupBatchSize || pendingQueue.size >= maxBufferedEnvelopes)
            triggerBatch()
          else if (!isTimerActive(BatchLingerTimerKey))
            scheduleOnce(BatchLingerTimerKey, batchLinger)
        }
      }

      // enforces the head-of-queue ordering invariant described in the class doc
      private def advance(): Unit = {
        var continue = true
        while (continue && pendingQueue.nonEmpty && !awaitingSnapshotLoad) {
          snapshotState.get(pendingQueue.head.persistenceId) match {
            case Some(s) =>
              val env = pendingQueue.dequeue()
              unpin(env.persistenceId)
              handleResolved(env, s) // may set awaitingSnapshotLoad = true, loop condition then stops us
            case None =>
              // head not resolved yet - nothing after it can be emitted without reordering
              continue = false
          }
        }
      }

      private def pullIfRoom(): Unit =
        if (!upstreamFinished && !hasBeenPulled(in) && (pendingQueue.size + readyQueue.size) < maxBufferedEnvelopes)
          pull(in)

      private def completeIfDone(): Unit =
        if (upstreamFinished && pendingQueue.isEmpty && pendingLookupIds.isEmpty && !batchInFlight &&
          !awaitingSnapshotLoad && readyQueue.isEmpty)
          completeStage()

      override def onPush(): Unit = {
        val env = grab(in)
        pendingQueue.enqueue(env)
        pin(env.persistenceId)
        if (!snapshotState.contains(env.persistenceId) && !idsInFlight.contains(env.persistenceId)) {
          pendingLookupIds += env.persistenceId
          maybeTriggerBatch()
        }
        advance()
        pullIfRoom()
      }

      override def onPull(): Unit = {
        if (readyQueue.nonEmpty)
          push(out, readyQueue.dequeue())
        // keep pulling upstream regardless, so refilling the buffer overlaps with downstream draining it
        // instead of the two phases alternating
        pullIfRoom()
        completeIfDone()
      }

      override def onUpstreamFinish(): Unit = {
        upstreamFinished = true
        maybeTriggerBatch()
        advance()
        completeIfDone()
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case BatchLingerTimerKey => triggerBatch()
        case _                   => ()
      }

      setHandler(in, this)
      setHandler(out, this)
    }

}
