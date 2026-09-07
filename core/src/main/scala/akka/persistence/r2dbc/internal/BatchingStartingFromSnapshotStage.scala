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
 * persistence ids than `cache-capacity`. In that situation the original stage's cache-miss rate approaches 1, and it
 * degrades into one fully serialized, blocking DB round-trip per event, because it does not pull the next upstream
 * element until the current lookup's callback has completed. Total pipeline throughput then becomes bound by round-trip
 * latency instead of DB or write capacity.
 *
 * This stage keeps the same LRU cache, but resolves a batch of cache misses with a single round-trip instead of one
 * round-trip per miss. All envelopes — cache hits included — pass through a single ordered `pendingQueue` and are only
 * ever decided (push / ignore / load snapshot) strictly from its head, one at a time:
 *   - every envelope is enqueued in arrival order, and if its persistence id isn't cached its id is added to a
 *     pending-lookup set
 *   - upstream keeps being pulled (bounded by `maxBufferedEnvelopes`) to keep the queue filling
 *   - once `lookupBatchSize` distinct new ids have accumulated (anywhere in the queue, not just at the head),
 *     `maxBufferedEnvelopes` is reached, a linger timer expires, or upstream finishes, a *single* batched lookup
 *     (`SnapshotDao.sequenceNumbersOfSnapshots`) resolves the whole pending set in one round-trip
 *   - the queue is then advanced from the head for as long as each head's persistence id is resolved; advancing stops
 *     the moment the head's id isn't resolved yet (nothing later in the queue can be emitted before it without
 *     reordering) or a snapshot payload load is in flight for the current head
 *
 * Every envelope, hit or miss, must pass through the same head-of-queue gate: deciding a cache-hit envelope out of
 * order, ahead of an earlier envelope still waiting on a lookup or a snapshot load, would reorder the stream.
 *
 * A persistence id that is resolved in the cache but still referenced by an envelope sitting in `pendingQueue` (stuck
 * behind an earlier, not-yet-resolved head) is pinned and exempt from LRU eviction until it is dequeued. Without this,
 * the entry could be evicted between being resolved and being dequeued, leaving that envelope permanently unresolved
 * and stalling the stage. The same applies while a snapshot payload load is in flight: the envelope that triggered it
 * has already been dequeued (and unpinned) by the time the load starts, but its persistence id must still not be
 * evicted for the load's duration - a batch lookup for a different id can run concurrently with the load (nothing gates
 * `triggerBatch` on `awaitingSnapshotLoad`), and if that eviction happened, a redelivery of the same id would see a
 * cache miss and start a redundant lookup racing the load, non-deterministically clobbering the `emitted` flag the load
 * is about to set and opening the door to a duplicate snapshot-envelope emission on a later redelivery.
 * `loadCorrespondingSnapshot` pins the id explicitly for exactly this reason.
 *
 * Loading the actual snapshot payload for entities whose boundary event is reached is left serialized as before — that
 * only happens once per entity (not once per cache eviction) so it isn't the dominant cost, but it is a natural next
 * step to pipeline the same way if it turns out to matter.
 *
 * Known gaps:
 *   - memory bound of the envelope buffer under pathological miss patterns not validated
 *   - snapshot-load step not pipelined (see above)
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
      // that a not-yet-dequeued envelope depends on so eviction can't invalidate a resolution before it's consumed
      private val pinCount = mutable.Map.empty[String, Int]
      // ids seen in pendingQueue that aren't cached yet and aren't already covered by an in-flight batch
      private val pendingLookupIds = mutable.LinkedHashSet.empty[String]
      // ids covered by the batch currently in flight, so a re-arriving envelope for the same id doesn't trigger
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
              // every cached entry is pinned (referenced by an envelope still sitting in pendingQueue behind a
              // not-yet-resolved head) - can't evict without risking losing a resolution before it's consumed.
              // Temporarily over cacheCapacity; bounded by maxBufferedEnvelopes.
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
        // pin explicitly: this envelope was already dequeued (and unpinned) by advance() before this is called,
        // but the cache entry it depends on must not be evicted while the load - a second async operation not
        // otherwise tracked by the queue - is in flight, or a concurrent batch resolving a different id could
        // evict it and a redelivery for this id would then race a redundant lookup against this load
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
          if (pendingLookupIds.size >= lookupBatchSize || pendingQueue.size >= maxBufferedEnvelopes)
            triggerBatch()
          else if (!isTimerActive(BatchLingerTimerKey))
            scheduleOnce(BatchLingerTimerKey, batchLinger)
        }
      }

      // advances strictly from the head: an envelope can only be decided once every envelope ahead of it
      // has already been decided, so a resolved (cache hit) id can never jump ahead of an earlier envelope
      // that is still waiting on a lookup or a snapshot load
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
        // flush whatever is pending rather than waiting for lookupBatchSize / the linger timer
        if (pendingLookupIds.nonEmpty && !batchInFlight)
          triggerBatch()
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
