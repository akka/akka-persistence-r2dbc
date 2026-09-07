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
 * Unit-level tests for the two correctness issues found (and fixed) in review: a resolved cache entry getting evicted
 * while still referenced by a queued envelope (deadlocks the stage), and a cache-hit envelope being decided while an
 * earlier envelope's snapshot payload load is still in flight (reorders the stream). Both are exercised here with fully
 * controlled, hand-completed `Promise`s so the exact interleaving that triggers them is deterministic, rather than
 * relying on timing against a real database.
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
      val e2 = createEnvelope(pidA, 2, "a2") // depends on pidA's cache entry surviving until it's dequeued

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
      // cacheCapacity = 1 so resolving pidB creates eviction pressure that would (bug) hit pidA's entry while
      // pidA's own snapshot payload load is in flight and unprotected by pinning - the load only pins ids
      // referenced by envelopes still sitting in the queue, but the envelope that triggered the load has
      // already been dequeued by the time the load starts. Uses Source.queue instead of a plain Vector source:
      // the stage pulls eagerly, so a plain source would let later envelopes race ahead and re-pin pidA before
      // the test gets a chance to evict it - offering elements one at a time under explicit test control avoids
      // that.
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
      // eB can't be decided yet - awaitingSnapshotLoad still blocks advance() until pidA's load completes below.
      // `success()` doesn't block until the stage has actually processed it (that happens asynchronously on the
      // stream's own dispatcher), so without waiting here, offer(e2) below could race ahead of the eviction
      // check this is meant to trigger. expectNoMessage doubles as that synchronization point.
      probe.expectNoMessage(100.millis)

      // only now, after pidA may have been evicted, does a redelivery for it arrive
      offer(e2)

      // without the pinning-during-load fix, pidA was evicted above and this re-triggers a redundant lookup
      // for it while the original load is still in flight; capture it now but resolve it only after the
      // original load below, so it's the *last* writer to pidA's cache entry - the interleaving that actually
      // corrupts the `emitted` flag (if the load's update wins here, the outcome would be correct by luck)
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
        // again, success() doesn't block until the stage has processed it - synchronize before offer(e3) below
        probe.expectNoMessage(100.millis)
      }

      // e3: a further redelivery: if the redundant batch above clobbered pidA's emitted flag back to false,
      // this triggers a second, duplicate snapshot load (and would eventually emit snapAEnvelope twice)
      offer(e3)
      val secondLoad = loadCalls.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS)
      secondLoad shouldBe null

      probe.cancel()
    }
  }
}
