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
 * SKETCH / PROTOTYPE — not wired into `R2dbcReadJournal` yet.
 *
 * Alternative to `StartingFromSnapshotStage` addressing the observed Westpac issue: with a cache-miss rate close to 1
 * (many more distinct persistence ids per projection partition than `cache-capacity`), the original stage degrades into
 * one fully serialized, blocking DB round-trip per event, because it does not pull the next upstream element until the
 * current lookup's callback has completed. Total pipeline throughput then becomes bound by round-trip latency instead
 * of DB or write capacity.
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
 * An earlier version of this stage had a fast path that decided cache-hit envelopes immediately, bypassing the queue.
 * That reordered the stream whenever a cache hit arrived while an async snapshot-payload load for an *earlier* envelope
 * was still in flight (caught by EventsBySliceStartingFromSnapshotSpec against real Postgres with batching enabled —
 * the first delivered envelope was a later plain event instead of the pending snapshot). Every envelope, hit or miss,
 * must now pass through the same head-of-queue gate.
 *
 * Loading the actual snapshot payload for entities whose boundary event is reached is left serialized as before — that
 * only happens once per entity (not once per cache eviction) so it isn't the dominant cost, but it is a natural next
 * step to pipeline the same way if it turns out to matter.
 *
 * Known gaps to close before this can replace the original stage:
 *   - no automated tests yet (should reuse/extend StartingFromSnapshotStageSpec)
 *   - config wiring for lookupBatchSize / maxBufferedEnvelopes / batchLinger not added to reference.conf
 *   - memory bound of the envelope buffer under pathological miss patterns not validated
 *   - snapshot-load step not pipelined (see above)
 */
@InternalApi private[r2dbc] object BatchingStartingFromSnapshotStage {
  private case class SnapshotState(seqNr: Long, emitted: Boolean)
  private case object BatchLingerTimerKey
}

/**
 * SKETCH / PROTOTYPE — see class doc.
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
      // ids seen in pendingQueue that aren't cached yet and aren't part of an in-flight batch
      private val pendingLookupIds = mutable.LinkedHashSet.empty[String]
      private var batchInFlight = false

      // envelopes that have been decided (should be pushed) but are waiting for downstream demand
      private val readyQueue = mutable.Queue.empty[EventEnvelope[Event]]

      private var awaitingSnapshotLoad = false
      private var upstreamFinished = false

      // for emitting heartbeat events
      private var filterCount = 0L
      private var latestTimestamp = Instant.EPOCH

      private def updateState(persistenceId: String, seqNr: Long, emitted: Boolean): Unit = {
        snapshotState = snapshotState.updated(persistenceId, SnapshotState(seqNr, emitted))
        recency.update(persistenceId)
        if (recency.size > cacheCapacity)
          recency.removeLeastRecent().foreach { pid =>
            snapshotState -= pid
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
        loadSnapshot(env.persistenceId)
          .map(result => (env, result))(ExecutionContext.parasitic)
          .onComplete(loadSnapshotCallback.invoke)
      }

      private val batchCallback = getAsyncCallback[Try[(Set[String], Map[String, Long])]] {
        case Success((ids, results)) =>
          batchInFlight = false
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
        if (!snapshotState.contains(env.persistenceId)) {
          pendingLookupIds += env.persistenceId
          maybeTriggerBatch()
        }
        advance()
        pullIfRoom()
      }

      override def onPull(): Unit = {
        if (readyQueue.nonEmpty)
          push(out, readyQueue.dequeue())
        else
          pullIfRoom()
        completeIfDone()
      }

      override def onUpstreamFinish(): Unit = {
        upstreamFinished = true
        // flush whatever is pending rather than waiting for lookupBatchSize / the linger timer
        if (pendingLookupIds.nonEmpty && !batchInFlight)
          triggerBatch()
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
