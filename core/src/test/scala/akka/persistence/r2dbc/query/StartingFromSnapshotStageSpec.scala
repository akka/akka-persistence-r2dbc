/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import scala.concurrent.Future

import org.scalatest.wordspec.AnyWordSpecLike

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

  def findEnvelope(persistenceId: PersistenceId, envelopes: Vector[EventEnvelope[Any]]): Option[EventEnvelope[Any]] =
    envelopes.find(env => env.persistenceId == persistenceId.id)

  def findEnvelope(
      persistenceId: PersistenceId,
      seqNr: Long,
      envelopes: Vector[EventEnvelope[Any]],
      source: String = ""): Option[EventEnvelope[Any]] =
    envelopes.find(env => env.persistenceId == persistenceId.id && env.sequenceNr == seqNr && env.source == source)

  "StartingFromSnapshotStage" must {
    "emit envelopes from snapshots and from ordinary events" in {
      val snapshotEnvelopes = Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidC, 1, "snap-c1"))

      val eventEnvelopes = Vector(
        createEnvelope(pidA, 1, "a1"),
        createEnvelope(pidA, 2, "a2"),
        createEnvelope(pidA, 3, "a3"),
        createEnvelope(pidA, 4, "a4"),
        createEnvelope(pidB, 1, "b1"),
        createEnvelope(pidB, 2, "b2"),
        createEnvelope(pidC, 1, "c1"),
        createEnvelope(pidC, 2, "c2"))

      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
        Future.successful(
          findEnvelope(PersistenceId.ofUniqueId(persistenceId), snapshotEnvelopes).map(createSerializedSnapshotRow))

      def createEnvelopeFromSnapshotRow(snap: SerializedSnapshotRow): EventEnvelope[Any] =
        findEnvelope(PersistenceId.ofUniqueId(snap.persistenceId), snap.seqNr, snapshotEnvelopes)
          .getOrElse(throw new IllegalArgumentException(s"Unknown envelope for [$snap]"))

      val source =
        Source(eventEnvelopes)
          .via(Flow.fromGraph(new StartingFromSnapshotStage(loadSnapshot, createEnvelopeFromSnapshotRow)))

      val probe = source.runWith(TestSink())
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

    "handle backtracking envelopes" in {
      val snapshotEnvelopes = Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidC, 1, "snap-c1"))

      val eventEnvelopes = Vector(
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

      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
        Future.successful(
          findEnvelope(PersistenceId.ofUniqueId(persistenceId), snapshotEnvelopes).map(createSerializedSnapshotRow))

      def createEnvelopeFromSnapshotRow(snap: SerializedSnapshotRow): EventEnvelope[Any] =
        findEnvelope(PersistenceId.ofUniqueId(snap.persistenceId), snap.seqNr, snapshotEnvelopes)
          .getOrElse(throw new IllegalArgumentException(s"Unknown envelope for [$snap]"))

      val source =
        Source(eventEnvelopes)
          .via(Flow.fromGraph(new StartingFromSnapshotStage(loadSnapshot, createEnvelopeFromSnapshotRow)))

      val probe = source.runWith(TestSink())
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

    "handle sequence gap" in {
      val snapshotEnvelopes = Vector(createEnvelope(pidA, 2, "snap-a2"), createEnvelope(pidB, 2, "snap-b2"))

      val eventEnvelopes = Vector(
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

      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
        Future.successful(
          findEnvelope(PersistenceId.ofUniqueId(persistenceId), snapshotEnvelopes).map(createSerializedSnapshotRow))

      def createEnvelopeFromSnapshotRow(snap: SerializedSnapshotRow): EventEnvelope[Any] =
        findEnvelope(PersistenceId.ofUniqueId(snap.persistenceId), snap.seqNr, snapshotEnvelopes)
          .getOrElse(throw new IllegalArgumentException(s"Unknown envelope for [$snap]"))

      val source =
        Source(eventEnvelopes)
          .via(Flow.fromGraph(new StartingFromSnapshotStage(loadSnapshot, createEnvelopeFromSnapshotRow)))

      val probe = source.runWith(TestSink())
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

    "fail if snapshots loading fail" in {
      val snapshotEnvelopes = Vector(createEnvelope(pidA, 2, "snap-a2"))

      val eventEnvelopes =
        Vector(createEnvelope(pidA, 1, "a1"), createEnvelope(pidA, 2, "a2"), createEnvelope(pidA, 3, "a3"))

      def loadSnapshot(persistenceId: String): Future[Option[SerializedSnapshotRow]] =
        Future.failed(new RuntimeException(s"Simulated exc when loading snapshot [$persistenceId]"))

      def createEnvelopeFromSnapshotRow(snap: SerializedSnapshotRow): EventEnvelope[Any] =
        findEnvelope(PersistenceId.ofUniqueId(snap.persistenceId), snap.seqNr, snapshotEnvelopes)
          .getOrElse(throw new IllegalArgumentException(s"Unknown envelope for [$snap]"))

      val source =
        Source(eventEnvelopes)
          .via(Flow.fromGraph(new StartingFromSnapshotStage(loadSnapshot, createEnvelopeFromSnapshotRow)))

      val probe = source.runWith(TestSink())
      probe.request(100)

      probe
        .expectError()
        .getMessage shouldBe s"Simulated exc when loading snapshot [${eventEnvelopes.head.persistenceId}]"
    }

  }

}
