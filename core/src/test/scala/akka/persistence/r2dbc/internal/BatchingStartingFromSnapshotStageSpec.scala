/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.typed.PersistenceId
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

/**
 * Unit-level tests for `BatchingStartingFromSnapshotStage`'s ordering, cache-eviction, and buffering invariants: a
 * resolved cache entry evicted while still referenced by a queued envelope, a cache-hit envelope decided while an
 * earlier envelope's snapshot payload load is still in flight, a cache entry evicted while its own payload load is in
 * flight, and the buffered-envelope count under a pattern of distinct, never-repeating persistence ids. These use fully
 * controlled, hand-completed `Promise`s so the exact interleaving that exercises each case is deterministic, rather
 * than relying on timing against a real database.
 */
class BatchingStartingFromSnapshotStageSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private val entityType = "TestEntity"
  private val persistence = Persistence(system)
  private implicit val sys: ActorSystem[_] = system

  private val pidA = PersistenceId(entityType, "a")
  private val pidB = PersistenceId(entityType, "b")

  private def createEnvelope(pid: PersistenceId, seqNr: Long, evt: String): EventEnvelope[Any] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      persistence.sliceForPersistenceId(pid.id),
      filtered = false,
      "",
      tags = Set.empty)
  }

  private def createSerializedSnapshotRow(pid: PersistenceId, seqNr: Long): SerializedSnapshotRow =
    SerializedSnapshotRow(
      persistence.sliceForPersistenceId(pid.id),
      entityType,
      pid.id,
      seqNr,
      Instant.now(),
      Instant.now().toEpochMilli,
      Array.empty,
      0,
      "",
      Set.empty,
      None)

  private def createHeartbeat(timestamp: Instant): EventEnvelope[Any] =
    new EventEnvelope(
      TimestampOffset(timestamp, Map.empty),
      "heartbeat-pid",
      1L,
      eventOption = None,
      timestamp.toEpochMilli,
      _eventMetadata = None,
      entityType,
      0,
      filtered = true,
      source = EnvelopeOrigin.SourceHeartbeat,
      Set.empty)

  private def stage(
      cacheCapacity: Int,
      lookupBatchSize: Int,
      sequenceNumbersOfSnapshots: Set[String] => Future[Map[String, Long]],
      loadSnapshot: String => Future[Option[SerializedSnapshotRow]],
      createEnvelopeFromSnapshot: (SerializedSnapshotRow, TimestampOffset) => EventEnvelope[Any],
      maxBufferedEnvelopes: Int = 100,
      batchLinger: FiniteDuration = 1.hour): Flow[EventEnvelope[Any], EventEnvelope[Any], NotUsed] =
    Flow.fromGraph(
      new BatchingStartingFromSnapshotStage[Any](
        cacheCapacity,
        lookupBatchSize,
        maxBufferedEnvelopes,
        batchLinger,
        sequenceNumbersOfSnapshots,
        loadSnapshot,
        createEnvelopeFromSnapshot,
        heartbeatAfter = 1000,
        createHeartbeat))

  // no dialect actually returns a snapshot for this pid; used where a lookup only needs to resolve, not load
  private def neverLoadsSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
    Future.successful(None)

  private def poll[A](queue: LinkedBlockingQueue[A]): A = {
    val v = queue.poll(5, TimeUnit.SECONDS)
    if (v == null) throw new AssertionError("expected a lookup call within 5 seconds but none arrived")
    v
  }

  "BatchingStartingFromSnapshotStage" must {

    "batch multiple distinct cache misses into a single lookup call" in {
      val lookupCalls = new AtomicInteger(0)
      def sequenceNumbersOfSnapshots(ids: Set[String]): Future[Map[String, Long]] = {
        lookupCalls.incrementAndGet()
        Future.successful(Map.empty) // no snapshots, both events pass straight through
      }

      val e1 = createEnvelope(pidA, 1, "a1")
      val e2 = createEnvelope(pidB, 1, "b1")

      val probe = Source(Vector(e1, e2))
        .via(
          stage(
            cacheCapacity = 10,
            lookupBatchSize = 2,
            sequenceNumbersOfSnapshots,
            neverLoadsSnapshot,
            (_, _) => throw new IllegalStateException("no snapshot expected")))
        .runWith(TestSink())

      probe.request(10)
      probe.expectNext(e1)
      probe.expectNext(e2)
      probe.expectComplete()

      lookupCalls.get() shouldBe 1
    }

    "not stall when a resolved entry pinned by a still-queued envelope would otherwise be evicted" in {
      // cacheCapacity = 1 so the second lookup's resolution creates immediate eviction pressure on the first
      val batchCalls = new LinkedBlockingQueue[(Set[String], Promise[Map[String, Long]])]()
      def sequenceNumbersOfSnapshots(ids: Set[String]): Future[Map[String, Long]] = {
        val p = Promise[Map[String, Long]]()
        batchCalls.put(ids -> p)
        p.future
      }

      val e0 = createEnvelope(pidA, 1, "a1") // resolves and is dequeued before the eviction pressure below
      val e1 = createEnvelope(pidB, 1, "b1") // sits ahead of e2 in the queue, unresolved, blocking advance()
      val e2 = createEnvelope(pidA, 2, "a2") // depends on pidA's cache entry surviving until it is dequeued

      val probe = Source(Vector(e0, e1, e2))
        .via(
          stage(
            cacheCapacity = 1,
            lookupBatchSize = 1,
            sequenceNumbersOfSnapshots,
            neverLoadsSnapshot,
            (_, _) => throw new IllegalStateException("no snapshot expected")))
        .runWith(TestSink())

      probe.request(10)

      val (ids0, p0) = poll(batchCalls)
      ids0 shouldBe Set(pidA.id)
      p0.success(Map.empty)
      probe.expectNext(e0)

      // without the eviction-pinning fix this hangs forever: resolving pidB pushes the (capacity 1) cache over
      // the edge, evicting pidA's just-resolved entry while e2 - which still needs it - sits queued behind e1
      val (ids1, p1) = poll(batchCalls)
      ids1 shouldBe Set(pidB.id)
      p1.success(Map.empty)

      probe.expectNext(e1)
      probe.expectNext(e2)
      probe.expectComplete()
    }

    "not decide a cache-hit envelope while an earlier envelope's snapshot load is still in flight" in {
      val snapAEnvelope = createEnvelope(pidA, 5, "snap-a5")

      val batchCalls = new LinkedBlockingQueue[(Set[String], Promise[Map[String, Long]])]()
      def sequenceNumbersOfSnapshots(ids: Set[String]): Future[Map[String, Long]] = {
        val p = Promise[Map[String, Long]]()
        batchCalls.put(ids -> p)
        p.future
      }

      val loadCalls = new LinkedBlockingQueue[(String, Promise[Option[SerializedSnapshotRow]])]()
      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] = {
        val p = Promise[Option[SerializedSnapshotRow]]()
        loadCalls.put(persistenceId -> p)
        p.future
      }

      // e1's persistence id has a snapshot at the same seqNr as the event itself, so resolving it triggers a
      // snapshot payload load rather than an immediate decision; e2's persistence id has no snapshot, so it is
      // immediately decidable once resolved - but must still wait behind e1 in the queue.
      val e1 = createEnvelope(pidA, 5, "a5")
      val e2 = createEnvelope(pidB, 1, "b1")

      val probe = Source(Vector(e1, e2))
        .via(
          stage(
            cacheCapacity = 10,
            lookupBatchSize = 2,
            sequenceNumbersOfSnapshots,
            loadSnapshot,
            (_, _) => snapAEnvelope))
        .runWith(TestSink())

      probe.request(10)

      val (ids, p) = poll(batchCalls)
      ids shouldBe Set(pidA.id, pidB.id)
      p.success(Map(pidA.id -> 5L)) // pidB has no snapshot, pidA's snapshot is exactly at e1's seqNr

      // pidB is fully resolved at this point, but must not be emitted ahead of pidA, which is still waiting on
      // its snapshot payload load
      probe.expectNoMessage(200.millis)

      val (loadPid, loadPromise) = poll(loadCalls)
      loadPid shouldBe pidA.id
      loadPromise.success(Some(createSerializedSnapshotRow(pidA, 5L)))

      probe.expectNext(snapAEnvelope)
      probe.expectNext(e2)
      probe.expectComplete()
    }

    "not let an id's cache entry be evicted while its snapshot payload load is in flight" in {
      // cacheCapacity = 1 so resolving pidB creates eviction pressure on pidA's entry while pidA's own load is
      // in flight. Uses Source.queue, not a plain source: the stage pulls eagerly, so a plain source would let
      // later envelopes race ahead and re-pin pidA before the test gets a chance to evict it.
      val snapAEnvelope = createEnvelope(pidA, 5, "snap-a5")

      val batchCalls = new LinkedBlockingQueue[(Set[String], Promise[Map[String, Long]])]()
      def sequenceNumbersOfSnapshots(ids: Set[String]): Future[Map[String, Long]] = {
        val p = Promise[Map[String, Long]]()
        batchCalls.put(ids -> p)
        p.future
      }

      val loadCalls = new LinkedBlockingQueue[(String, Promise[Option[SerializedSnapshotRow]])]()
      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] = {
        val p = Promise[Option[SerializedSnapshotRow]]()
        loadCalls.put(persistenceId -> p)
        p.future
      }

      val e1 = createEnvelope(pidA, 5, "a5") // triggers pidA's snapshot load
      val eB = createEnvelope(pidB, 1, "b1") // unrelated id; resolving it creates the eviction pressure
      val e2 = createEnvelope(pidA, 5, "a5-redelivered") // arrives only after pidA has (bug) been evicted
      val e3 = createEnvelope(pidA, 5, "a5-redelivered-again") // a further, later redelivery of the same event

      val (queue, probe) = Source
        .queue[EventEnvelope[Any]](16, OverflowStrategy.fail)
        .via(
          stage(
            cacheCapacity = 1,
            lookupBatchSize = 1,
            sequenceNumbersOfSnapshots,
            loadSnapshot,
            (_, _) => snapAEnvelope,
            maxBufferedEnvelopes = 16))
        .toMat(TestSink())(Keep.both)
        .run()

      def offer(env: EventEnvelope[Any]): Unit = Await.result(queue.offer(env), 5.seconds)

      probe.request(10)

      // e1: pidA is uncached, resolves to snapshot seqNr 5 (== e1's own seqNr), which triggers the load
      offer(e1)
      val (aIds, aPromise) = poll(batchCalls)
      aIds shouldBe Set(pidA.id)
      aPromise.success(Map(pidA.id -> 5L))
      val (loadPid1, loadPromise1) = poll(loadCalls)
      loadPid1 shouldBe pidA.id
      // do not complete this load yet - pidA is now unpinned (e1 was its only queue reference) while the
      // load stays in flight

      // eB: unrelated id, resolves with no snapshot - at cacheCapacity 1 this creates eviction pressure, and
      // pidA (unpinned, untouched since its load started) is the only eviction candidate
      offer(eB)
      val (bIds, bPromise) = poll(batchCalls)
      bIds shouldBe Set(pidB.id)
      bPromise.success(Map.empty)
      // eB cannot be decided yet - awaitingSnapshotLoad blocks it. success() does not block until the stage has
      // processed it, so this wait is needed or offer(e2) below could race ahead of the eviction it should see.
      probe.expectNoMessage(100.millis)

      // only now, after pidA may have been evicted, does a redelivery for it arrive
      offer(e2)

      // captured now, but resolved only after the load below, so it is the last writer to pidA's entry - the
      // interleaving that actually corrupts `emitted` (if the load wins instead, the outcome is correct by luck)
      val redundantBatch = batchCalls.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS)

      // complete the original load - decides e1 (and unblocks eB and e2, queued behind it)
      loadPromise1.success(Some(createSerializedSnapshotRow(pidA, 5L)))
      probe.expectNext(snapAEnvelope)
      probe.expectNext(eB)
      // e2 is a redelivery of the same seqNr as the already-emitted snapshot - must be ignored, not re-loaded
      probe.expectNoMessage(200.millis)

      if (redundantBatch != null) {
        val (ids, p) = redundantBatch
        ids shouldBe Set(pidA.id)
        p.success(Map(pidA.id -> 5L)) // same seqNr as before - simulates the DB state being unchanged
        // again, success() does not block until the stage has processed it - synchronize before offer(e3) below
        probe.expectNoMessage(100.millis)
      }

      // e3: a further redelivery: if the redundant batch above clobbered pidA's emitted flag back to false,
      // this triggers a second, duplicate snapshot load (and would eventually emit snapAEnvelope twice)
      offer(e3)
      val secondLoad = loadCalls.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS)
      secondLoad shouldBe null

      probe.cancel()
    }

    "bound the number of buffered envelopes to maxBufferedEnvelopes even when every persistence id is distinct and nothing ever resolves" in {
      // adversarial case for the envelope buffer: every envelope is for a different, never-repeating persistence
      // id, and no lookup is ever completed, so nothing is ever decided and nothing ever drains - the buffer can
      // only stop growing because upstream demand stops, not because anything gets consumed
      val maxBuffered = 200
      val lookupBatchSize = 50

      val requestedIds = new LinkedBlockingQueue[Set[String]]()
      def sequenceNumbersOfSnapshots(ids: Set[String]): Future[Map[String, Long]] = {
        requestedIds.put(ids)
        Promise[Map[String, Long]]().future // never completes
      }

      val (queue, probe) = Source
        .queue[EventEnvelope[Any]](1, OverflowStrategy.backpressure)
        .via(stage(
          cacheCapacity = 10,
          lookupBatchSize = lookupBatchSize,
          sequenceNumbersOfSnapshots,
          neverLoadsSnapshot,
          (_, _) => throw new IllegalStateException("no snapshot expected"),
          maxBufferedEnvelopes = maxBuffered))
        .toMat(TestSink())(Keep.both)
        .run()

      probe.request(10000)

      // bufferSize 1 with backpressure means offer() only completes once the stage has pulled room for it, so
      // this measures how many envelopes the stage buffers before it stops. The +1 below is the queue's own
      // single-element buffer, not slack in the stage's bound.
      var offered = 0
      var blocked = false
      while (!blocked && offered < maxBuffered * 3) {
        val env = createEnvelope(PersistenceId(entityType, s"distinct-$offered"), 1, "e")
        try {
          Await.result(queue.offer(env), 300.millis)
          offered += 1
        } catch {
          case _: java.util.concurrent.TimeoutException => blocked = true
        }
      }

      blocked shouldBe true
      offered shouldBe maxBuffered + 1

      var totalRequestedIds = Set.empty[String]
      var batch = requestedIds.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
      while (batch != null) {
        totalRequestedIds ++= batch
        batch = requestedIds.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
      }
      totalRequestedIds.size should be <= maxBuffered

      probe.cancel()
    }
  }
}
