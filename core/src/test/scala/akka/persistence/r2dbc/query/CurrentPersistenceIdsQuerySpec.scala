/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

class CurrentPersistenceIdsQuerySpec
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

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private val zeros = "0000"
  private val numberOfEntityTypes = 5
  private val entityTypes = (1 to numberOfEntityTypes).map(n => "Entity" + zeros.drop(n.toString.length) + n)
  private val numberOfPids = 100
  private val pids = {
    (1 to numberOfEntityTypes).flatMap(entityTypeId =>
      (1 to numberOfPids / numberOfEntityTypes).map(n =>
        PersistenceId(entityTypes(entityTypeId - 1), "p" + zeros.drop(n.toString.length) + n)))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val probe = createTestProbe[Done]()
    pids.foreach { pid =>
      val persister = spawn(TestActors.Persister(pid))
      persister ! Persister.PersistWithAck("e-1", probe.ref)
      persister ! Persister.PersistWithAck("e-2", probe.ref)
      persister ! Persister.Stop(probe.ref)
    }

    probe.receiveMessages(numberOfPids * 3, 30.seconds) // 2 acks + stop done
  }

  "Event Sourced currentPersistenceIds" should {
    "retrieve all ids" in {
      val result = query.currentPersistenceIds().runWith(Sink.seq).futureValue
      result shouldBe pids.map(_.id)
    }

    "retrieve ids after id" in {
      val result = query.currentPersistenceIds(afterId = Some(pids(9).id), limit = 7).runWith(Sink.seq).futureValue
      result shouldBe pids.slice(10, 17).map(_.id)
    }

    "retrieve ids for entity type" in {
      val entityType = entityTypes(1)
      val result =
        query.currentPersistenceIds(entityType = entityType, afterId = None, limit = 30).runWith(Sink.seq).futureValue
      result shouldBe pids.filter(_.entityTypeHint == entityType).map(_.id)
    }

    "retrieve ids for entity type after id" in {
      val entityType = entityTypes(0)
      val result =
        query
          .currentPersistenceIds(entityType = entityType, afterId = Some(pids(9).id), limit = 7)
          .runWith(Sink.seq)
          .futureValue

      result shouldBe pids.filter(_.entityTypeHint == entityType).slice(10, 17).map(_.id)
    }

  }

}
