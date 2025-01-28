/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.journal.R2dbcJournal
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.scaladsl.Recovery
import akka.serialization.SerializationExtension

class LastEventByPersistenceIdSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private val serialization = SerializationExtension(system)

  "internalLastEventByPersistenceId" should {
    "return last event" in {
      val pid = nextPid()
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      val events = (1 to 3).map { i =>
        val payload = s"e-$i"
        persister ! PersistWithAck(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }

      val serializedRow = query.internalLastEventByPersistenceId(pid, Long.MaxValue, includeDeleted = true).futureValue
      serializedRow.isDefined shouldBe true
      val repr = R2dbcJournal.deserializeRow(serialization, serializedRow.get)
      repr.payload shouldBe events.last
      repr.sequenceNr shouldBe 3L
    }

    "return None when not exists" in {
      val pid = nextPid()
      val serializedRow = query.internalLastEventByPersistenceId(pid, Long.MaxValue, includeDeleted = true).futureValue
      serializedRow shouldBe None
    }

    "return a specific event of the toSequenceNr" in {
      val pid = nextPid()
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      val events = (1 to 3).map { i =>
        val payload = s"e-$i"
        persister ! PersistWithAck(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }

      val serializedRow =
        query.internalLastEventByPersistenceId(pid, toSequenceNr = 2L, includeDeleted = true).futureValue
      serializedRow.isDefined shouldBe true
      val repr = R2dbcJournal.deserializeRow(serialization, serializedRow.get)
      repr.payload shouldBe events(1)
      repr.sequenceNr shouldBe 2L
    }

    "include metadata" in {
      val probe = testKit.createTestProbe[Done]()
      val entityType = nextEntityType()
      val entityId = "entity-1"
      val pid = TestActors.replicatedEventSourcedPersistenceId(entityType, entityId).id

      val persister = testKit.spawn(TestActors.replicatedEventSourcedPersister(entityType, entityId))
      persister ! Persister.PersistWithAck("e-1", probe.ref)
      probe.expectMessage(Done)
      persister ! Persister.PersistWithAck("e-2", probe.ref)
      probe.expectMessage(Done)

      val serializedRow = query.internalLastEventByPersistenceId(pid, Long.MaxValue, includeDeleted = true).futureValue
      serializedRow.isDefined shouldBe true
      val repr = R2dbcJournal.deserializeRow(serialization, serializedRow.get)

      val meta2 = repr.metadata.get.asInstanceOf[ReplicatedEventMetadata]
      meta2.originReplica.id shouldBe "dc-1"
      meta2.originSequenceNr shouldBe 2L
    }
  }

  "EventSourcedBehavior with Recovery." should {
    "recover from last event" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      val persister = testKit.spawn(Persister.withRecovery(pid, Recovery.replayOnlyLast))
      (1 to 3).foreach { i =>
        val payload = s"e-$i"
        persister ! Persister.Persist(payload)
      }

      val stateProbe = createTestProbe[String]()
      persister ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("e-1|e-2|e-3")

      testKit.stop(persister)

      val persister2 = testKit.spawn(Persister.withRecovery(pid, Recovery.replayOnlyLast))
      persister2 ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage("e-3")
    }
  }

}
