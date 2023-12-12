/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.pubsub.Topic
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.PublishEventsDynamicSettings
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PubSub extends ExtensionId[PubSub] {
  private val log = LoggerFactory.getLogger(classOf[PubSub])

  def createExtension(system: ActorSystem[_]): PubSub = new PubSub(system)

  // Java API
  def get(system: ActorSystem[_]): PubSub = apply(system)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class PubSub(system: ActorSystem[_]) extends Extension {
  import PubSub.log

  private val topics = new ConcurrentHashMap[String, ActorRef[Any]]
  private val persistenceExt = Persistence(system)

  private val settings = new PublishEventsDynamicSettings(
    system.settings.config.getConfig("akka.persistence.r2dbc.journal.publish-events-dynamic"))

  private val sliceRanges = {
    val numberOfTopics = system.settings.config.getInt("akka.persistence.r2dbc.journal.publish-events-number-of-topics")
    persistenceExt.sliceRanges(numberOfTopics)
  }
  private val sliceRangeLookup = new ConcurrentHashMap[Int, Range]

  private val throughputCollectIntervalMillis = settings.throughputCollectInterval.toMillis
  private val throughputThreshold = settings.throughputThreshold.toDouble
  private val throughputSampler =
    math.min(1000, math.max(1, settings.throughputThreshold / 10)) // 1/10 of threshold, but between 1-1000
  private val throughputCounter = new AtomicLong
  @volatile private var throughput =
    EWMA(0.0, EWMA.alpha(settings.throughputCollectInterval * 2, settings.throughputCollectInterval))

  def eventTopic[Event](entityType: String, slice: Int): ActorRef[Topic.Command[EventEnvelope[Event]]] = {
    val name = topicName(entityType, slice)
    topics
      .computeIfAbsent(name, _ => system.systemActorOf(Topic[EventEnvelope[Event]](name), name).unsafeUpcast[Any])
      .narrow[Topic.Command[EventEnvelope[Event]]]
  }

  def eventTopics[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int): Set[ActorRef[Topic.Command[EventEnvelope[Event]]]] = {
    (minSlice to maxSlice).map(eventTopic[Event](entityType, _)).toSet
  }

  private def topicName(entityType: String, slice: Int): String = {
    val range = sliceRangeLookup.computeIfAbsent(
      slice,
      _ =>
        sliceRanges
          .find(_.contains(slice))
          .getOrElse(throw new IllegalArgumentException(s"Slice [$slice] not found in " +
          s"slice ranges [${sliceRanges.mkString(", ")}]")))
    URLEncoder.encode(s"r2dbc-$entityType-${range.min}-${range.max}", StandardCharsets.UTF_8.name())
  }

  def publish(pr: PersistentRepr, timestamp: Instant): Unit = {
    updateThroughput()

    if (throughput.value < throughputThreshold) {
      val pid = pr.persistenceId
      val entityType = PersistenceId.extractEntityType(pid)
      val slice = persistenceExt.sliceForPersistenceId(pid)

      val offset = TimestampOffset(timestamp, timestamp, Map(pid -> pr.sequenceNr))

      val (eventOption, tags, filtered) = pr.payload match {
        case Tagged(payload, tags) =>
          (Some(payload), tags, false)
        case FilteredPayload =>
          (None, Set.empty[String], true)
        case other =>
          (Some(other), Set.empty[String], false)
      }

      val envelope = new EventEnvelope(
        offset,
        pid,
        pr.sequenceNr,
        eventOption,
        timestamp.toEpochMilli,
        pr.metadata,
        entityType,
        slice,
        filtered,
        source = EnvelopeOrigin.SourcePubSub,
        tags)

      publishToTopic(envelope)
    }
  }

  def publish(envelope: EventEnvelope[Any]): Unit = {
    updateThroughput()

    if (throughput.value < throughputThreshold)
      publishToTopic(envelope)
  }

  private def publishToTopic(envelope: EventEnvelope[Any]): Unit = {
    val entityType = PersistenceId.extractEntityType(envelope.persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(envelope.persistenceId)

    eventTopic(entityType, slice) ! Topic.Publish(envelope)
  }

  private def updateThroughput(): Unit = {
    val n = throughputCounter.incrementAndGet()
    if (n % throughputSampler == 0) {
      val ewma = throughput
      val durationMillis = (System.nanoTime() - ewma.nanoTime) / 1000 / 1000
      if (durationMillis >= throughputCollectIntervalMillis) {
        // doesn't have to be exact so "missed" or duplicate concurrent calls don't matter
        throughputCounter.set(0L)
        val rps = n * 1000.0 / durationMillis
        val newEwma = ewma :+ rps
        throughput = newEwma
        if (ewma.value < throughputThreshold && newEwma.value >= throughputThreshold) {
          log.info("Disabled publishing of events. Throughput greater than [{}] events/s", throughputThreshold)
        } else if (ewma.value >= throughputThreshold && newEwma.value < throughputThreshold) {
          log.info("Enabled publishing of events. Throughput less than [{}] events/s", throughputThreshold)
        } else {
          log.debug(
            "Publishing of events is {}. Throughput is [{}] events/s",
            if (newEwma.value < throughputThreshold) "enabled" else "disabled",
            newEwma.value)
        }
      }
    }
  }
}
