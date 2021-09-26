/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.Done
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{ Effect, EventSourcedBehavior }

object TestActors {
  object Persister {
    sealed trait Command
    final case class Persist(payload: Any) extends Command
    final case class PersistWithAck(payload: Any, replyTo: ActorRef[Done]) extends Command
    final case class PersistAll(payloads: List[Any]) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: String): Behavior[Command] =
      EventSourcedBehavior[Command, Any, String](
        persistenceId = PersistenceId.ofUniqueId(pid),
        "",
        { (_, command) =>
          command match {
            case command: Persist =>
              Effect.persist(command.payload)
            case command: PersistWithAck =>
              Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
            case command: PersistAll =>
              Effect.persist(command.payloads)
            case Ping(replyTo) =>
              replyTo ! Done
              Effect.none
            case Stop(replyTo) =>
              replyTo ! Done
              Effect.stop()
          }
        },
        (_, _) => "")
  }
}
