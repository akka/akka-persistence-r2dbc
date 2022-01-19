/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

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
import akka.persistence.r2dbc.TestActors.Persister.PersistAll
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySlicePubSubSpec {
  def config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      journal.publish-events = on
      # no events from database query, only via pub-sub
      query.behind-current-time = 5 minutes
    }
    """)
    .withFallback(TestConfig.backtrackingDisabledConfig.withFallback(TestConfig.config))
}

class EventsBySlicePubSubSpec
    extends ScalaTestWithActorTestKit(EventsBySlicePubSubSpec.config)
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
    val probe = createTestProbe[Done]
    val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)
  }

  s"EventsBySlices pub-sub" should {

    "publish new events" in new Setup {

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](entityType, slice, slice, NoOffset).runWith(sinkProbe).request(10)

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      for (i <- 1 to 20) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }

      // 10 was requested
      for (i <- 1 to 10) {
        result.expectNext().event shouldBe s"e-$i"
      }
      result.expectNoMessage()

      result.request(100)
      for (i <- 11 to 20) {
        result.expectNext().event shouldBe s"e-$i"
      }

      for (i <- 21 to 30) {
        persister ! Persist(s"e-$i")
        result.expectNext().event shouldBe s"e-$i"
      }

      persister ! PersistAll(List("e-31", "e-32", "e-33"))
      for (i <- 31 to 33) {
        result.expectNext().event shouldBe s"e-$i"
      }

      result.cancel()
    }

  }

}
