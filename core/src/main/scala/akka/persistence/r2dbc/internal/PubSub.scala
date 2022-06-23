/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.pubsub.Topic
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.journal.Tagged
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PubSub extends ExtensionId[PubSub] {
  def createExtension(system: ActorSystem[_]): PubSub = new PubSub(system)

  // Java API
  def get(system: ActorSystem[_]): PubSub = apply(system)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class PubSub(system: ActorSystem[_]) extends Extension {
  private val topics = new ConcurrentHashMap[String, ActorRef[Any]]
  private val persistenceExt = Persistence(system)

  def eventTopic[Event](entityType: String, slice: Int): ActorRef[Topic.Command[EventEnvelope[Event]]] = {
    val name = topicName(entityType, slice)
    topics
      .computeIfAbsent(name, _ => system.systemActorOf(Topic[EventEnvelope[Event]](name), name).unsafeUpcast[Any])
      .narrow[Topic.Command[EventEnvelope[Event]]]
  }

  private def topicName(entityType: String, slice: Int): String =
    URLEncoder.encode(s"r2dbc-$entityType-$slice", StandardCharsets.UTF_8.name())

  def publish(pr: PersistentRepr, timestamp: Instant): Unit = {
    val pid = pr.persistenceId
    val entityType = PersistenceId.extractEntityType(pid)
    val slice = persistenceExt.sliceForPersistenceId(pid)

    val offset = TimestampOffset(timestamp, timestamp, Map(pid -> pr.sequenceNr))
    val payload =
      pr.payload match {
        case Tagged(payload, _) =>
          // eventsByTag not implemented (see issue #82), but events can still be tagged, so we unwrap this tagged event.
          payload

        case other => other
      }

    val envelope = new EventEnvelope(
      offset,
      pid,
      pr.sequenceNr,
      Option(payload),
      timestamp.toEpochMilli,
      pr.metadata,
      entityType,
      slice)
    eventTopic(entityType, slice) ! Topic.Publish(envelope)
  }
}
