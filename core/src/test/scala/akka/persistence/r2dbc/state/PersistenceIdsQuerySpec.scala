/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

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

class PersistenceIdsQuerySpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
        akka.persistence.r2dbc.query.persistence-ids.buffer-size = 20
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
  private val entityTypeHint = nextEntityTypeHint()
  private val numberOfPids = 100
  private val pids =
    (1 to numberOfPids).map(n => PersistenceId(entityTypeHint, "p" + zeros.drop(n.toString.length) + n))

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val probe = createTestProbe[Done]
    pids.foreach { pid =>
      val persister = spawn(TestActors.DurableStatePersister(pid))
      persister ! DurableStatePersister.PersistWithAck("s-1", probe.ref)
      persister ! DurableStatePersister.Stop(probe.ref)
    }

    probe.receiveMessages(numberOfPids * 2, 30.seconds) // ack + stop done
  }

  "Durable State persistenceIds" should {
    "retrieve all ids" in {
      val result = store.persistenceIds().runWith(Sink.seq).futureValue
      result shouldBe pids.map(_.id)
    }

    "retrieve ids afterId" in {
      val result = store.currentPersistenceIds(afterId = Some(pids(9).id), limit = 7).runWith(Sink.seq).futureValue
      result shouldBe pids.slice(10, 17).map(_.id)
    }

  }

}
