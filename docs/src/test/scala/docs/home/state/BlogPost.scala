/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.state

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect

object BlogPost {
  val EntityTypeName = "BlogPost"

  sealed trait State

  case object BlankState extends State

  final case class DraftState(content: PostContent) extends State {
    def withBody(newBody: String): DraftState =
      copy(content = content.copy(body = newBody))

    def postId: String = content.postId
  }

  final case class PublishedState(content: PostContent) extends State {
    def postId: String = content.postId
  }

  sealed trait Command
  final case class AddPost(content: PostContent, replyTo: ActorRef[StatusReply[AddPostDone]]) extends Command
  final case class AddPostDone(postId: String)
  final case class GetPost(replyTo: ActorRef[PostContent]) extends Command
  final case class ChangeBody(newBody: String, replyTo: ActorRef[Done]) extends Command
  final case class Publish(replyTo: ActorRef[Done]) extends Command
  final case class PostContent(postId: String, title: String, body: String)

  def apply(entityId: String, persistenceId: PersistenceId): Behavior[Command] = {
    Behaviors.setup { context =>
      context.log.info("Starting BlogPostEntityDurableState {}", entityId)
      DurableStateBehavior[Command, State](persistenceId, emptyState = BlankState, commandHandler)
    }
  }

  private val commandHandler: (State, Command) => Effect[State] = { (state, command) =>
    state match {

      case BlankState =>
        command match {
          case cmd: AddPost => addPost(cmd)
          case _            => Effect.unhandled
        }

      case draftState: DraftState =>
        command match {
          case cmd: ChangeBody  => changeBody(draftState, cmd)
          case Publish(replyTo) => publish(draftState, replyTo)
          case GetPost(replyTo) => getPost(draftState, replyTo)
          case AddPost(_, replyTo) =>
            Effect.unhandled[State].thenRun(_ => replyTo ! StatusReply.Error("Cannot add post while in draft state"))
        }

      case publishedState: PublishedState =>
        command match {
          case GetPost(replyTo) => getPost(publishedState, replyTo)
          case AddPost(_, replyTo) =>
            Effect.unhandled[State].thenRun(_ => replyTo ! StatusReply.Error("Cannot add post, already published"))
          case _ => Effect.unhandled
        }
    }
  }

  private def addPost(cmd: AddPost): Effect[State] = {
    Effect.persist(DraftState(cmd.content)).thenRun { _ =>
      cmd.replyTo ! StatusReply.Success(AddPostDone(cmd.content.postId))
    }
  }

  private def changeBody(state: DraftState, cmd: ChangeBody): Effect[State] = {
    Effect.persist(state.withBody(cmd.newBody)).thenRun { _ =>
      cmd.replyTo ! Done
    }
  }

  private def publish(state: DraftState, replyTo: ActorRef[Done]): Effect[State] = {
    Effect.persist(PublishedState(state.content)).thenRun { _ =>
      println(s"Blog post ${state.postId} was published")
      replyTo ! Done
    }
  }

  private def getPost(state: DraftState, replyTo: ActorRef[PostContent]): Effect[State] = {
    replyTo ! state.content
    Effect.none
  }

  private def getPost(state: PublishedState, replyTo: ActorRef[PostContent]): Effect[State] = {
    replyTo ! state.content
    Effect.none
  }

}
