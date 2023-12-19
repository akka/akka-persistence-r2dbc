/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import org.scalatest.concurrent.ScalaFutures.convertScalaFuture

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.pubsub.Topic
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateUpdateWithChangeEventStore
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck

class DurableStateUpdateWithChangeEventStoreSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val store = DurableStateStoreRegistry(system)
    .durableStateStoreFor[DurableStateUpdateWithChangeEventStore[String]](R2dbcDurableStateStore.Identifier)
  private val journalQuery =
    PersistenceQuery(system).readJournalFor[CurrentEventsByPersistenceIdTypedQuery](R2dbcReadJournal.Identifier)

  private val tag = "TAG"

  "The R2DBC durable state store" should {
    "save additional change event" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "my-persistenceId").id
      val value1 = "Genuinely Collaborative"
      val value2 = "Open to Feedback"

      store.upsertObject(persistenceId, 1L, value1, tag, s"Changed to $value1").futureValue
      store.upsertObject(persistenceId, 2L, value2, tag, s"Changed to $value2").futureValue
      store.deleteObject(persistenceId, 3L, "Deleted").futureValue

      val envelopes = journalQuery
        .currentEventsByPersistenceIdTyped[String](persistenceId, 1L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue

      val env1 = envelopes.head
      env1.event shouldBe s"Changed to $value1"
      env1.sequenceNr shouldBe 1L
      env1.tags shouldBe Set(tag)

      val env2 = envelopes(1)
      env2.event shouldBe s"Changed to $value2"
      env2.sequenceNr shouldBe 2L

      val env3 = envelopes(2)
      env3.event shouldBe s"Deleted"
      env3.sequenceNr shouldBe 3L
    }

    "save additional change event in same transaction" in {
      // test rollback (same tx) if the journal insert fails via simulated unique constraint violation in event_journal
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "my-persistenceId").id

      val probe = testKit.createTestProbe[Done]()
      val persister = testKit.spawn(Persister(persistenceId))
      persister ! PersistWithAck("a", probe.ref)
      probe.expectMessage(Done)
      testKit.stop(persister)

      val value1 = "Genuinely Collaborative"

      store.upsertObject(persistenceId, 1L, value1, tag, s"Changed to $value1").failed.futureValue

      store.getObject(persistenceId).futureValue.value shouldBe None
    }

    "detect and reject concurrent inserts, and not store change event" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-inserted-concurrently").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, revision = 1L, value, tag, s"Changed to $value").futureValue

      val updatedValue = "Open to Feedback"
      store
        .upsertObject(persistenceId, revision = 1L, updatedValue, tag, s"Changed to $updatedValue")
        .failed
        .futureValue

      val envelopes = journalQuery
        .currentEventsByPersistenceIdTyped[String](persistenceId, 1L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue
      envelopes.size shouldBe 1
    }

    "detect and reject concurrent updates, and not store change event" in {
      if (!r2dbcSettings.durableStateAssertSingleWriter)
        pending

      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, revision = 1L, value, tag, s"Changed to $value").futureValue

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId, revision = 2L, updatedValue, tag, s"Changed to $updatedValue").futureValue

      // simulate an update by a different node that didn't see the first one:
      val updatedValue2 = "Genuine and Sincere in all Communications"
      store
        .upsertObject(persistenceId, revision = 2L, updatedValue2, tag, s"Changed to $updatedValue2")
        .failed
        .futureValue

      val envelopes = journalQuery
        .currentEventsByPersistenceIdTyped[String](persistenceId, 1L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue
      envelopes.size shouldBe 2
    }

    "detect and reject concurrent delete of revision 1, and not store change event" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-deleted-concurrently").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, revision = 1L, value, tag, s"Changed to $value").futureValue

      store.deleteObject(persistenceId, revision = 1L, "Deleted").failed.futureValue

      val envelopes = journalQuery
        .currentEventsByPersistenceIdTyped[String](persistenceId, 1L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue
      envelopes.size shouldBe 1
    }

    "detect and reject concurrent deletes, and not store change event" in {
      if (!r2dbcSettings.durableStateAssertSingleWriter)
        pending

      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, revision = 1L, value, tag, s"Changed to $value").futureValue

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId, revision = 2L, updatedValue, tag, s"Changed to $updatedValue").futureValue

      // simulate a delete by a different node that didn't see the first one:
      store.deleteObject(persistenceId, revision = 2L, "Deleted").failed.futureValue

      val envelopes = journalQuery
        .currentEventsByPersistenceIdTyped[String](persistenceId, 1L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue
      envelopes.size shouldBe 2
    }

  }

  "publish change event" in {
    val entityType = nextEntityType()
    val persistenceId = PersistenceId(entityType, "my-persistenceId").id

    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val topic = PubSub(system).eventTopic[String](entityType, slice)
    val subscriberProbe = createTestProbe[EventEnvelope[String]]()
    topic ! Topic.Subscribe(subscriberProbe.ref)

    val value1 = "Genuinely Collaborative"
    val value2 = "Open to Feedback"

    store.upsertObject(persistenceId, 1L, value1, tag, s"Changed to $value1").futureValue
    store.upsertObject(persistenceId, 2L, value2, tag, s"Changed to $value2").futureValue
    store.deleteObject(persistenceId, 3L, "Deleted").futureValue

    val env1 = subscriberProbe.receiveMessage()
    env1.event shouldBe s"Changed to $value1"
    env1.sequenceNr shouldBe 1L
    env1.tags shouldBe Set(tag)
    env1.source shouldBe EnvelopeOrigin.SourcePubSub

    val env2 = subscriberProbe.receiveMessage()
    env2.event shouldBe s"Changed to $value2"
    env2.sequenceNr shouldBe 2L

    val env3 = subscriberProbe.receiveMessage()
    env3.event shouldBe s"Deleted"
    env3.sequenceNr shouldBe 3L
  }

}
