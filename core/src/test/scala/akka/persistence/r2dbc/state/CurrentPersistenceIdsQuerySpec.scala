/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.DurableStatePersister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

class CurrentPersistenceIdsQuerySpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
        akka.persistence.r2dbc {
          query.persistence-ids.buffer-size = 20
          state {
            custom-table {
              "CustomEntity" = durable_state_test
            }
          }
        }
        """)
        .withFallback(TestConfig.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val store = DurableStateStoreRegistry(testKit.system)
    .durableStateStoreFor[R2dbcDurableStateStore[String]](R2dbcDurableStateStore.Identifier)

  private val zeros = "0000"
  private val numberOfEntityTypes = 5
  private val entityTypes = (1 to numberOfEntityTypes).map(_ => nextEntityType())
  private val numberOfPids = 100

  private val pids = {
    (1 to numberOfEntityTypes).flatMap(entityTypeId =>
      (1 to numberOfPids / numberOfEntityTypes).map(n =>
        PersistenceId(entityTypes(entityTypeId - 1), "p" + zeros.drop(n.toString.length) + n)))
  }

  private val customTable = r2dbcSettings.getDurableStateTableWithSchema("CustomEntity")
  private val customEntityType = "CustomEntity"
  private val customPid1 = nextPid(customEntityType)
  private val customPid2 = nextPid(customEntityType)

  private val createTable = if (r2dbcSettings.dialectName == "sqlserver") {
    s"IF object_id('$customTable') is null SELECT * into $customTable from durable_state where persistence_id = ''"
  } else {
    s"create table if not exists $customTable as select * from durable_state where persistence_id = ''"
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    Await.result(
      r2dbcExecutor.executeDdl("beforeAll create durable_state_test")(_.createStatement(createTable)),
      20.seconds)

    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from $customTable")),
      10.seconds)

    val probe = createTestProbe[Done]()
    pids.foreach { pid =>
      val persister = spawn(TestActors.DurableStatePersister(pid))
      persister ! DurableStatePersister.PersistWithAck("s-1", probe.ref)
      persister ! DurableStatePersister.Stop(probe.ref)
    }

    probe.receiveMessages(numberOfPids * 2, 30.seconds) // ack + stop done
  }

  private def createPidsInCustomTable(): Unit = {
    val probe = createTestProbe[Done]()
    val persister1 = spawn(TestActors.DurableStatePersister(customPid1))
    persister1 ! DurableStatePersister.Persist("s-1")
    persister1 ! DurableStatePersister.Stop(probe.ref)
    val persister2 = spawn(TestActors.DurableStatePersister(customPid2))
    persister2 ! DurableStatePersister.Persist("s-1")
    persister2 ! DurableStatePersister.Stop(probe.ref)
    probe.expectMessage(Done)
    probe.expectMessage(Done)
  }

  "Durable State persistenceIds" should {
    "retrieve all ids" in {
      val result = store.currentPersistenceIds().runWith(Sink.seq).futureValue
      result shouldBe pids.map(_.id)
    }

    "retrieve ids afterId" in {
      val result = store.currentPersistenceIds(afterId = Some(pids(9).id), limit = 7).runWith(Sink.seq).futureValue
      result shouldBe pids.slice(10, 17).map(_.id)
    }

    "retrieve ids for entity type" in {
      val entityType = entityTypes(1)
      val result =
        store.currentPersistenceIds(entityType = entityType, afterId = None, limit = 30).runWith(Sink.seq).futureValue
      result shouldBe pids.filter(_.entityTypeHint == entityType).map(_.id)
    }

    "retrieve ids for entity type after id" in {
      val entityType = entityTypes(0)
      val result =
        store
          .currentPersistenceIds(entityType = entityType, afterId = Some(pids(9).id), limit = 7)
          .runWith(Sink.seq)
          .futureValue

      result shouldBe pids.filter(_.entityTypeHint == entityType).slice(10, 17).map(_.id)
    }

    "include pids from custom table in all ids" in {
      createPidsInCustomTable()
      val result = store.currentPersistenceIds().runWith(Sink.seq).futureValue
      // note that custom tables always come afterwards, i.e. not strictly sorted on the pids (but that should be ok)
      result shouldBe (pids.map(_.id) :+ customPid1 :+ customPid2)
    }

    "include pids from custom table in ids afterId" in {
      createPidsInCustomTable()
      val result1 = store.currentPersistenceIds(afterId = Some(pids(9).id), limit = 1000).runWith(Sink.seq).futureValue
      // custom pids not included because "CustomEntity" < "TestEntity"
      result1 shouldBe pids.drop(10).map(_.id)

      val result2 = store.currentPersistenceIds(afterId = Some(customPid1), limit = 1000).runWith(Sink.seq).futureValue
      result2 shouldBe customPid2 +: pids.map(_.id)
    }

    "include pids from custom table in ids for entity type" in {
      createPidsInCustomTable()
      val result =
        store
          .currentPersistenceIds(entityType = customEntityType, afterId = None, limit = 30)
          .runWith(Sink.seq)
          .futureValue
      result shouldBe Vector(customPid1, customPid2)
    }

    "include pids from custom table in ids for entity type after id" in {
      createPidsInCustomTable()
      val result =
        store
          .currentPersistenceIds(entityType = customEntityType, afterId = Some(customPid1), limit = 7)
          .runWith(Sink.seq)
          .futureValue

      result shouldBe Vector(customPid2)
    }

  }

}
