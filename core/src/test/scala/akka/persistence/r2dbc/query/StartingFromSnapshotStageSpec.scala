/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.StartingFromSnapshotStage
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike

class StartingFromSnapshotStageSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private val entityType = "TestEntity"
  private val persistence = Persistence(system)
  private implicit val sys: ActorSystem[_] = system

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
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
      source = "",
      tags = tags)
  }

  private val snapshotEnvelopes = Vector(
    createEnvelope(PersistenceId(entityType, "a"), 2, "snap-a2"),
    createEnvelope(PersistenceId(entityType, "b"), 3, "snap-b3"),
    createEnvelope(PersistenceId(entityType, "c"), 1, "snap-c1"))

  private val primaryEnvelopes = Vector(
    createEnvelope(PersistenceId(entityType, "a"), 3, "a3"),
    createEnvelope(PersistenceId(entityType, "a"), 4, "a4"),
    createEnvelope(PersistenceId(entityType, "b"), 4, "b4"))

  "StartingFromSnapshotStage" must {
    "emit envelopes from snapshots and then from primary" in {
      val source =
        Source.fromGraph(new StartingFromSnapshotStage(Source(snapshotEnvelopes), _ => Source(primaryEnvelopes)))

      val probe = source.runWith(TestSink())
      probe.request(100)
      probe.expectNextN(snapshotEnvelopes ++ primaryEnvelopes)
      probe.expectComplete()
    }

    "collect offsets from snapshots" in {
      val source =
        Source.fromGraph(
          new StartingFromSnapshotStage(
            Source(snapshotEnvelopes),
            snapshotOffsets => {
              val moreEnvelopes =
                snapshotOffsets.iterator
                  .map { case (pid, (seqNr, _)) =>
                    createEnvelope(
                      PersistenceId.ofUniqueId(pid),
                      seqNr + 1,
                      s"${PersistenceId.ofUniqueId(pid).entityId}${seqNr + 1}")
                  }
                  .toVector
                  .sortBy(_.persistenceId)
              Source(moreEnvelopes)
            }))

      val probe = source.runWith(TestSink())
      probe.request(100)
      probe.expectNextN(snapshotEnvelopes)
      probe.expectNext().event shouldBe "a3"
      probe.expectNext().event shouldBe "b4"
      probe.expectNext().event shouldBe "c2"
    }

    "fail if snapshots fail" in {
      val source =
        Source.fromGraph(
          new StartingFromSnapshotStage(Source.failed(TestException("err")), _ => Source(primaryEnvelopes)))

      val probe = source.runWith(TestSink())
      probe.request(100)
      probe.expectError(TestException("err"))
    }

    "fail if primary fail" in {
      val source =
        Source.fromGraph(new StartingFromSnapshotStage(Source.empty, _ => Source.failed(TestException("err"))))

      val probe = source.runWith(TestSink())
      probe.request(100)
      probe.expectError(TestException("err"))
    }
  }

}
