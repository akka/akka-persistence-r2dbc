/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

object EventsBySlicePubSubBacktrackingSpec {
  def config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      journal.publish-events = on
      query {
        refresh-interval = 1 s

        # Too make the test predictable, PubSub arriving first
        behind-current-time = 2 s

        backtracking {
          behind-current-time = 3 s
          # enough space for heartbeats (previous query - behind current time)
          window = 6 s
        }
      }
    }
    akka.actor.testkit.typed.filter-leeway = 20.seconds
    """)
    .withFallback(TestConfig.config)
}

class EventsBySlicePubSubBacktrackingSpec
    extends ScalaTestWithActorTestKit(EventsBySlicePubSubBacktrackingSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPid(entityType)
    val slice = query.sliceForPersistenceId(persistenceId)
    val persister = spawn(TestActors.Persister(persistenceId))
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)
  }

  s"EventsBySlices pub-sub with backtracking enabled" should {

    "publish events" in new Setup {

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](this.entityType, slice, slice, NoOffset).runWith(sinkProbe).request(1000)

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      for (i <- 1 to 9) {
        persister ! Persist(s"e-$i")
      }
      persister ! PersistWithAck("e-10", probe.ref)
      probe.expectMessage(Done)
      // Initial PubSub events are dropped because no backtracking events yet
      result.expectNoMessage(500.millis)

      for (i <- 1 to 10) {
        val env = result.expectNext()
        env.event shouldBe s"e-$i"
        env.source shouldBe EnvelopeOrigin.SourceQuery
      }

      result.expectNoMessage(1.second)
      for (i <- 1 to 10) {
        val env = result.expectNext()
        env.sequenceNr shouldBe i
        env.source shouldBe EnvelopeOrigin.SourceBacktracking
      }

      // after backtracking the PubSub events will get through
      for (i <- 11 to 19) {
        persister ! Persist(s"e-$i")
      }
      persister ! PersistWithAck("e-20", probe.ref)
      for (i <- 11 to 20) {
        val env = result.expectNext()
        env.event shouldBe s"e-$i"
        env.source shouldBe EnvelopeOrigin.SourcePubSub
      }
      // and then the ordinary query
      for (i <- 11 to 20) {
        val env = result.expectNext()
        env.event shouldBe s"e-$i"
        env.source shouldBe EnvelopeOrigin.SourceQuery
      }
      // and backtracking
      for (i <- 11 to 20) {
        val env = result.expectNext()
        env.sequenceNr shouldBe i
        env.source shouldBe EnvelopeOrigin.SourceBacktracking
      }

      // after idle it will emit heartbeat
      Thread.sleep(6000)

      // and because of the heartbeat it will accept PubSub even though it's now > backtracking.window
      persister ! PersistWithAck("e-21", probe.ref)

      {
        val env = result.expectNext()
        env.event shouldBe s"e-21"
        env.source shouldBe EnvelopeOrigin.SourcePubSub
      }

      result.cancel()
    }

  }

}
