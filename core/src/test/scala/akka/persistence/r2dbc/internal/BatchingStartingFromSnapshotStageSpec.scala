/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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
import akka.stream.scaladsl.Flow
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
  }
}
