/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.home.journal

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

object BlogPost {
  val EntityType = "BlogPost"

  sealed trait Command
  final case class AddPost(postId: String, title: String, body: String, replyTo: ActorRef[Done]) extends Command
  final case class ChangeBody(newBody: String, replyTo: ActorRef[Done]) extends Command
  final case class Publish(replyTo: ActorRef[Done]) extends Command

  sealed trait Event
  final case class PostAdded(postId: String, title: String, body: String) extends Event
  final case class BodyChanged(postId: String, newBody: String) extends Event
  final case class Published(postId: String) extends Event

  final case class State(postId: String, title: String, body: String, published: Boolean)

  def apply(persistenceId: PersistenceId): Behavior[Command] =
    EventSourcedBehavior[Command, Event, Option[State]](
      persistenceId,
      emptyState = None,
      commandHandler = { (state, command) =>
        command match {
          case AddPost(postId, title, body, replyTo) =>
            Effect.persist(PostAdded(postId, title, body)).thenReply(replyTo)(_ => Done)
          case ChangeBody(newBody, replyTo) =>
            state match {
              case Some(s) => Effect.persist(BodyChanged(s.postId, newBody)).thenReply(replyTo)(_ => Done)
              case None    => Effect.reply(replyTo)(Done)
            }
          case Publish(replyTo) =>
            state match {
              case Some(s) => Effect.persist(Published(s.postId)).thenReply(replyTo)(_ => Done)
              case None    => Effect.reply(replyTo)(Done)
            }
        }
      },
      eventHandler = { (state, event) =>
        event match {
          case PostAdded(postId, title, body) => Some(State(postId, title, body, published = false))
          case BodyChanged(_, newBody)        => state.map(_.copy(body = newBody))
          case Published(_)                   => state.map(_.copy(published = true))
        }
      })
}
