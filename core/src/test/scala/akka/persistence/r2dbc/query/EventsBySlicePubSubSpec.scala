/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestActors.Persister.PersistAll
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.typed.scaladsl.ActorFlow
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.persistence.SerializedEvent
import akka.serialization.SerializationExtension
import akka.serialization.Serializers

object EventsBySlicePubSubSpec {
  def config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      journal.publish-events = on
      #journal.publish-events-number-of-topics = 4
      journal.publish-events-dynamic {
        throughput-threshold = 50
        throughput-collect-interval = 1 second
      }

      # no events from database query, only via pub-sub
      query.behind-current-time = 5 minutes
    }
    akka.actor.testkit.typed.filter-leeway = 20.seconds
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
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)
  }

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      time: Instant = Instant.now()): EventEnvelope[String] = {
    EventEnvelope(
      TimestampOffset(time, time, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      time.toEpochMilli,
      pid.entityTypeHint,
      query.sliceForPersistenceId(pid.id),
      filtered = false,
      source = EnvelopeOrigin.SourcePubSub)
  }

  def backtrackingEnvelope(env: EventEnvelope[String]): EventEnvelope[String] =
    new EventEnvelope[String](
      env.offset,
      env.persistenceId,
      env.sequenceNr,
      eventOption = None,
      env.timestamp,
      env.eventMetadata,
      env.entityType,
      env.slice,
      env.filtered,
      source = EnvelopeOrigin.SourceBacktracking)

  private val entityType = nextEntityType()
  private val pidA = PersistenceId(entityType, "A")
  private val pidB = PersistenceId(entityType, "B")
  val now = Instant.now()
  private val envA1 = createEnvelope(pidA, 1L, "a1", now)
  private val envA2 = createEnvelope(pidA, 2L, "a2", now.plusMillis(1))
  private val envA3 = createEnvelope(pidA, 3L, "a3", now.plusMillis(2))
  private val envB1 = createEnvelope(pidB, 1L, "b1", now.plusMillis(3))
  private val envB2 = createEnvelope(pidB, 2L, "b2", now.plusMillis(4))

  s"EventsBySlices pub-sub" should {

    "publish new events" in new Setup {

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](this.entityType, slice, slice, NoOffset).runWith(sinkProbe).request(10)

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      for (i <- 1 to 20) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }

      // 10 was requested
      for (i <- 1 to 10) {
        val env = result.expectNext()
        env.event shouldBe s"e-$i"
        env.source shouldBe EnvelopeOrigin.SourcePubSub
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

      result.expectNoMessage()

      result.cancel()
    }

    "deduplicate" in {
      val out = Source(List(envA1, envA2, envB1, envA3, envA1, envA2, envB1, envA3, envB2, envB2))
        .via(query.deduplicate(capacity = 10))
        .runWith(Sink.seq)
        .futureValue
      out shouldBe List(envA1, envA2, envB1, envA3, envB2)
    }

    "not deduplicate from backtracking" in {
      val envA2back = backtrackingEnvelope(envA2)
      val out = Source(List(envA1, envA2, envB1, envA2back, envB2))
        .via(query.deduplicate(capacity = 10))
        .runWith(Sink.seq)
        .futureValue
      out shouldBe List(envA1, envA2, envB1, envA2back, envB2)
    }

    "evict oldest from deduplication cache" in {
      val out = Source(List(envA1, envA2, envA3, envB1, envB1, envA2, envA1, envB2, envA1))
        .via(query.deduplicate(capacity = 3))
        .runWith(Sink.seq)
        .futureValue
      out shouldBe List(envA1, envA2, envA3, envB1, envA1, envB2) // envA1 was evicted and therefore duplicate
    }

    "turn FilteredPayload to filtered events with no payload" in new Setup {
      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](this.entityType, 0, 1023, NoOffset).runWith(sinkProbe).request(10)

      // make sure subscription completed
      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      // publish filtered payload
      PubSub(system).publish(PersistentRepr(FilteredPayload, 0L, PersistenceId(this.entityType, "1").id), Instant.now())

      val envelope: EventEnvelope[String] = result.expectNext()
      envelope.filtered should be(true)
      envelope.eventOption should be(empty)
    }

    "skipPubSubTooFarAhead" in {
      val (in, out) =
        TestSource[EventEnvelope[String]]()
          .via(
            query.skipPubSubTooFarAhead(
              enabled = true,
              maxAheadOfBacktracking = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)))
          .toMat(TestSink[EventEnvelope[String]]())(Keep.both)
          .run()
      out.request(100)
      in.sendNext(envA1)
      in.sendNext(envA2)

      // all pubsub events dropped before the first backtracking event
      out.expectNoMessage()

      val pidC = PersistenceId(entityType, "C")
      in.sendNext(backtrackingEnvelope(envA1))
      out.expectNext(backtrackingEnvelope(envA1))
      // now the pubsub event is passed through
      in.sendNext(envB1)
      out.expectNext(envB1)

      val time2 = envA1.offset
        .asInstanceOf[TimestampOffset]
        .timestamp
        .plusMillis(settings.querySettings.backtrackingWindow.toMillis)
      val envC1 = createEnvelope(pidC, 1L, "c1", time2.plusMillis(1))
      val envC2 = createEnvelope(pidC, 2L, "c2", time2.plusMillis(2))
      in.sendNext(envC1)
      // dropped because > backtrackingWindow
      out.expectNoMessage()

      in.sendNext(backtrackingEnvelope(envB1))
      out.expectNext(backtrackingEnvelope(envB1))
      in.sendNext(envC2)
      out.expectNext(envC2)
    }

    "dynamically enable/disable publishing based on throughput" in new Setup {
      import akka.actor.typed.scaladsl.adapter._

      val consumerProbe = createTestProbe[EventEnvelope[String]]()

      query
        .eventsBySlices[String](this.entityType, slice, slice, NoOffset)
        .runWith(
          Sink.actorRef(consumerProbe.ref.toClassic, onCompleteMessage = "done", onFailureMessage = _.getMessage))

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      for (i <- 1 to 10) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }

      consumerProbe.receiveMessages(10)

      LoggingTestKit.info("Disabled publishing of events").expect {
        val done1 = Source(11 to 600)
          .throttle(200, 1.second)
          .via(ActorFlow.ask[Int, PersistWithAck, Done](1)(persister) { case (i, replyTo) =>
            PersistWithAck(s"e-$i", replyTo)
          })
          .runWith(Sink.ignore)

        Await.result(done1, 20.seconds)
      }

      var count = 0
      var lookForMore = true
      while (lookForMore) {
        try {
          consumerProbe.receiveMessage(1.second)
          count += 1
        } catch {
          case _: AssertionError => lookForMore = false // timeout
        }
      }
      count should be <= 500

      LoggingTestKit.info("Enabled publishing of events").expect {
        val done2 = Source(601 to 800)
          .throttle(20, 1.second)
          .via(ActorFlow.ask[Int, PersistWithAck, Done](1)(persister) { case (i, replyTo) =>
            PersistWithAck(s"e-$i", replyTo)
          })
          .runWith(Sink.ignore)

        Await.result(done2, 20.seconds)
      }

    }

    "group slices into topics" in new Setup {

      val numberOfTopics =
        typedSystem.settings.config.getInt("akka.persistence.r2dbc.journal.publish-events-number-of-topics")
      //
      val querySliceRanges = Persistence(typedSystem).sliceRanges(numberOfTopics * 2)
      val queries: immutable.IndexedSeq[TestSubscriber.Probe[EventEnvelope[String]]] = {
        querySliceRanges.map { range =>
          query.eventsBySlices[String](this.entityType, range.min, range.max, NoOffset).runWith(sinkProbe).request(100)
        }
      }

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        (0 until 1024).foreach { i =>
          withClue(s"slice $i: ") {
            PubSub(typedSystem).eventTopic[String](this.entityType, i) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
            topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 2
          }
        }
      }

      for (i <- 1 to 10) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }

      for (i <- 1 to 10) {
        val queryIndex = querySliceRanges.indexOf(querySliceRanges.find(_.contains(slice)).get)
        queries(queryIndex).expectNext().event shouldBe s"e-$i"
      }

      queries.foreach(_.cancel())
    }

    "publish SerializedEvent" in new Setup {

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](this.entityType, slice, slice, NoOffset).runWith(sinkProbe).request(10)

      val topicStatsProbe = createTestProbe[TopicImpl.TopicStats]()
      eventually {
        PubSub(typedSystem).eventTopic[String](this.entityType, slice) ! TopicImpl.GetTopicStats(topicStatsProbe.ref)
        topicStatsProbe.receiveMessage().localSubscriberCount shouldBe 1
      }

      val event = "e1"
      val serializer = SerializationExtension(system).findSerializerFor(event)
      val serializedEvent =
        new SerializedEvent(
          serializer.toBinary(event),
          serializer.identifier,
          Serializers.manifestFor(serializer, event))

      persister ! PersistWithAck(serializedEvent, probe.ref)
      probe.expectMessage(Done)

      val env = result.expectNext()
      env.event shouldBe event
      env.source shouldBe EnvelopeOrigin.SourcePubSub

      result.cancel()
    }

  }

}
