/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import org.scalatest.wordspec.AnyWordSpecLike

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.internal.StartingFromSnapshotStage
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

class StartingFromSnapshotStageSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private val entityType = "TestEntity"
  private val persistence = Persistence(system)
  private implicit val sys: ActorSystem[_] = system

  private val pidA = PersistenceId(entityType, "a")
  private val pidB = PersistenceId(entityType, "b")
  private val pidC = PersistenceId(entityType, "c")

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      source: String = "",
      tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      persistence.sliceForPersistenceId(pid.id),
      filtered = false,
      source,
      tags = tags)
  }

  private def createSerializedSnapshotRow(env: EventEnvelope[Any]) =
    SerializedSnapshotRow(
      env.slice,
      env.entityType,
      env.persistenceId,
      env.sequenceNr,
      env.offset.asInstanceOf[TimestampOffset].timestamp,
      env.timestamp,
      Array.empty,
      0,
      "",
      Set.empty,
      None)

  def findEnvelope(
      persistenceId: PersistenceId,
      envelopes: IndexedSeq[EventEnvelope[Any]]): Option[EventEnvelope[Any]] =
    envelopes.find(env => env.persistenceId == persistenceId.id)

  def findEnvelope(
      persistenceId: PersistenceId,
      seqNr: Long,
      envelopes: IndexedSeq[EventEnvelope[Any]],
      source: String = ""): Option[EventEnvelope[Any]] =
    envelopes.find(env => env.persistenceId == persistenceId.id && env.sequenceNr == seqNr && env.source == source)

  private abstract class Setup {
    def snapshotEnvelopes: IndexedSeq[EventEnvelope[Any]]
    def eventEnvelopes: IndexedSeq[EventEnvelope[Any]]

    def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
      Future.successful(
        findEnvelope(PersistenceId.ofUniqueId(persistenceId), snapshotEnvelopes).map(createSerializedSnapshotRow))

    def sequenceNumberOfSnapshot(persistenceId: String): Future[Option[Long]] =
      Future.successful(findEnvelope(PersistenceId.ofUniqueId(persistenceId), snapshotEnvelopes).map(_.sequenceNr))

    def createEnvelopeFromSnapshotRow(snap: SerializedSnapshotRow, offset: TimestampOffset): EventEnvelope[Any] =
      findEnvelope(PersistenceId.ofUniqueId(snap.persistenceId), snap.seqNr, snapshotEnvelopes)
        .getOrElse(throw new IllegalArgumentException(s"Unknown envelope for [$snap]"))

    def source(cacheCapacity: Int = 10000): Source[EventEnvelope[Any], NotUsed] =
      Source(eventEnvelopes)
        .via(
          Flow.fromGraph(
            new StartingFromSnapshotStage(
              cacheCapacity,
              sequenceNumberOfSnapshot,
              loadSnapshot,
              createEnvelopeFromSnapshotRow)))
  }

  "StartingFromSnapshotStage" must {
    "emit envelopes from snapshots and from ordinary events" in new Setup {
      override lazy val snapshotEnvelopes =
        Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidC, 1, "snap-c1"))

      override lazy val eventEnvelopes = Vector(
        createEnvelope(pidA, 1, "a1"),
        createEnvelope(pidA, 2, "a2"),
        createEnvelope(pidA, 3, "a3"),
        createEnvelope(pidA, 4, "a4"),
        createEnvelope(pidB, 1, "b1"),
        createEnvelope(pidB, 2, "b2"),
        createEnvelope(pidC, 1, "c1"),
        createEnvelope(pidC, 2, "c2"))

      val probe = source().runWith(TestSink())
      probe.request(100)

      probe.expectNext(findEnvelope(pidA, 2, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 3, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 4, eventEnvelopes).get)

      probe.expectNext(findEnvelope(pidB, 1, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 2, eventEnvelopes).get)

      probe.expectNext(findEnvelope(pidC, 1, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidC, 2, eventEnvelopes).get)

      probe.expectComplete()
    }

    "handle backtracking envelopes" in new Setup {
      override lazy val snapshotEnvelopes =
        Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidC, 1, "snap-c1"))

      override lazy val eventEnvelopes = Vector(
        createEnvelope(pidA, 1, "a1"),
        createEnvelope(pidA, 1, "bt-a1", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidA, 2, "a2"),
        createEnvelope(pidA, 2, "bt-a2", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidA, 3, "a3"),
        createEnvelope(pidA, 4, "a4"),
        createEnvelope(pidA, 3, "bt-a3", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidA, 4, "bt-a4", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidB, 1, "b1"),
        createEnvelope(pidB, 2, "b2"),
        createEnvelope(pidB, 1, "bt-b1", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidB, 2, "bt-b2", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidC, 1, "c1"),
        createEnvelope(pidC, 1, "c1", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidC, 2, "c2"),
        createEnvelope(pidC, 2, "c2", source = EnvelopeOrigin.SourceBacktracking))

      val probe = source().runWith(TestSink())
      probe.request(100)

      probe.expectNext(findEnvelope(pidA, 2, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 3, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 4, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 3, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)
      probe.expectNext(findEnvelope(pidA, 4, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)

      probe.expectNext(findEnvelope(pidB, 1, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 2, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 1, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)
      probe.expectNext(findEnvelope(pidB, 2, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)

      probe.expectNext(findEnvelope(pidC, 1, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidC, 2, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidC, 2, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)

      probe.expectComplete()
    }

    "handle sequence gap" in new Setup {
      override lazy val snapshotEnvelopes =
        Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidB, 2, "snap-b2"))

      override lazy val eventEnvelopes = Vector(
        createEnvelope(pidA, 1, "a1"),
        // a2 is missing
        createEnvelope(pidA, 3, "a3"),
        createEnvelope(pidA, 2, "bt-a2", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidA, 4, "a4"),
        createEnvelope(pidA, 3, "bt-a3", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidA, 4, "bt-a4", source = EnvelopeOrigin.SourceBacktracking),

        // first is ahead of snapshot
        createEnvelope(pidB, 3, "b3"),
        createEnvelope(pidB, 4, "b4"),
        createEnvelope(pidB, 2, "bt-b2", source = EnvelopeOrigin.SourceBacktracking),
        createEnvelope(pidB, 3, "bt-b3", source = EnvelopeOrigin.SourceBacktracking))

      val probe = source().runWith(TestSink())
      probe.request(100)

      // a3 still emitted, since it's ahead of snapshot
      probe.expectNext(findEnvelope(pidA, 3, eventEnvelopes).get)

      // snapshot triggered by backtracking
      probe.expectNext(findEnvelope(pidA, 2, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 4, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidA, 3, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)
      probe.expectNext(findEnvelope(pidA, 4, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)

      probe.expectNext(findEnvelope(pidB, 3, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 4, eventEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 2, snapshotEnvelopes).get)
      probe.expectNext(findEnvelope(pidB, 3, eventEnvelopes, source = EnvelopeOrigin.SourceBacktracking).get)

      probe.expectComplete()
    }

    "fail if snapshots loading fail" in new Setup {
      override lazy val snapshotEnvelopes = Vector(createEnvelope(pidA, 2, "snap-a2"))

      override lazy val eventEnvelopes =
        Vector(createEnvelope(pidA, 1, "a1"), createEnvelope(pidA, 2, "a2"), createEnvelope(pidA, 3, "a3"))

      override def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
        Future.failed(new RuntimeException(s"Simulated exc when loading snapshot [$persistenceId]"))

      val probe = source().runWith(TestSink())
      probe.request(100)

      probe
        .expectError()
        .getMessage shouldBe s"Simulated exc when loading snapshot [${eventEnvelopes.head.persistenceId}]"
    }

  }

  "evict cache" in new Setup {
    val pids = (1 to 10).map { n =>
      PersistenceId(entityType, s"pid-$n")
    }

    override lazy val snapshotEnvelopes =
      pids.map(pid => createEnvelope(pid, 3, s"snap-3"))

    override lazy val eventEnvelopes =
      for {
        seqNr <- (1 to 5)
        pid <- pids
      } yield {
        createEnvelope(pid, seqNr, s"evt-$seqNr")
      }

    val loadCounter = new AtomicInteger

    override def sequenceNumberOfSnapshot(persistenceId: String): Future[Option[Long]] = {
      loadCounter.incrementAndGet()
      super.sequenceNumberOfSnapshot(persistenceId)
    }

    override def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] = {
      loadCounter.incrementAndGet()
      super.loadSnapshot(persistenceId)
    }

    val probe = source(cacheCapacity = 3).runWith(TestSink())
    probe.request(100)

    pids.foreach { pid =>
      probe.expectNext(findEnvelope(pid, 3, snapshotEnvelopes).get)
    }

    pids.foreach { pid =>
      probe.expectNext(findEnvelope(pid, 4, eventEnvelopes).get)
    }
    pids.foreach { pid =>
      probe.expectNext(findEnvelope(pid, 5, eventEnvelopes).get)
    }

    probe.expectComplete()

    // Normally, each pid snapshot is loaded twice, on firs occurrence, and when emitted.
    // When evicted from the cache, it is loaded again
    loadCounter.get() should be > (pids.size * 2)
  }

  "emit many events" in new Setup {
    override lazy val snapshotEnvelopes = Vector(createEnvelope(pidA, 33, "snap-33"))

    override lazy val eventEnvelopes = (1 to 100).map(seqNr => createEnvelope(pidA, seqNr, s"evt-$seqNr"))

    val probe = source().runWith(TestSink())
    probe.request(200)
    val received = probe.expectNextN(100 - 32)
    received.head shouldBe findEnvelope(pidA, snapshotEnvelopes).get
    probe.expectComplete()
  }

}
