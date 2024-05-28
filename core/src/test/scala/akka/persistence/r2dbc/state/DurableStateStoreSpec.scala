/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.PersistenceId
import org.scalatest.wordspec.AnyWordSpecLike

class DurableStateStoreSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val store = DurableStateStoreRegistry(testKit.system)
    .durableStateStoreFor[R2dbcDurableStateStore[String]](R2dbcDurableStateStore.Identifier)

  private val unusedTag = "n/a"

  "The R2DBC durable state store" should {
    "save and retrieve a value" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "my-persistenceId").id
      val value = "Genuinely Collaborative"

      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))
    }

    "produce None when fetching a non-existing key" in {
      val entityType = nextEntityType()
      val key = PersistenceId(entityType, "nonexistent-id").id
      store.getObject(key).futureValue should be(GetObjectResult(None, 0L))
    }

    "update a value" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-updated").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId, 2L, updatedValue, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(updatedValue), 2L))
    }

    "detect and reject concurrent inserts" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-inserted-concurrently")
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId.id, revision = 1L, value, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "Open to Feedback"
      val failure =
        store.upsertObject(persistenceId.id, revision = 1L, updatedValue, entityType).failed.futureValue
      failure.getMessage should include(
        s"Insert failed: durable state for persistence id [${persistenceId.id}] already exists")
    }

    "detect and reject concurrent updates" in {
      if (!settings.durableStateAssertSingleWriter)
        pending

      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently")
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId.id, revision = 1L, value, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId.id, revision = 2L, updatedValue, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(updatedValue), 2L))

      // simulate an update by a different node that didn't see the first one:
      val updatedValue2 = "Genuine and Sincere in all Communications"
      val failure =
        store.upsertObject(persistenceId.id, revision = 2L, updatedValue2, entityType).failed.futureValue
      failure.getMessage should include(
        s"Update failed: durable state for persistence id [${persistenceId.id}] could not be updated to revision [2]")
    }

    "hard delete when revision=0" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "to-be-added-and-removed").id
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))
      store.deleteObject(persistenceId, revision = 0).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 0L))
    }

    "delete payload but keep revision" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "to-be-added-and-removed").id
      val value1 = "value1"
      store.upsertObject(persistenceId, 1L, value1, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value1), 1L))
      store.deleteObject(persistenceId, revision = 2L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 2L))

      val value2 = "value2"
      store.upsertObject(persistenceId, 3L, value2, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value2), 3L))
    }

    "update revision when deleting" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "to-be-removed").id

      store.deleteObject(persistenceId, revision = 1L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 1L))
      store.deleteObject(persistenceId, revision = 2L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 2L))

      val value1 = "value1"
      store.upsertObject(persistenceId, 3L, value1, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value1), 3L))

      store.deleteObject(persistenceId, revision = 4L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 4L))
      store.deleteObject(persistenceId, revision = 5L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 5L))
    }

    "detect and reject concurrent delete of revision 1" in {
      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-deleted-concurrently")
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId.id, revision = 1L, value, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(value), 1L))

      val failure =
        store.deleteObject(persistenceId.id, revision = 1L).failed.futureValue
      failure.getMessage should include(
        s"Insert delete marker with revision 1 failed: durable state for persistence id [${persistenceId.id}] already exists")
    }

    "detect and reject concurrent deletes" in {
      if (!settings.durableStateAssertSingleWriter)
        pending

      val entityType = nextEntityType()
      val persistenceId = PersistenceId(entityType, "id-to-be-updated-concurrently")
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId.id, revision = 1L, value, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId.id, revision = 2L, updatedValue, entityType).futureValue
      store.getObject(persistenceId.id).futureValue should be(GetObjectResult(Some(updatedValue), 2L))

      // simulate a delete by a different node that didn't see the first one:
      val updatedValue2 = "Genuine and Sincere in all Communications"
      val failure =
        store.deleteObject(persistenceId.id, revision = 2L).failed.futureValue
      failure.getMessage should include(
        s"Delete failed: durable state for persistence id [${persistenceId.id}] could not be updated to revision [2]")
    }

  }

}
