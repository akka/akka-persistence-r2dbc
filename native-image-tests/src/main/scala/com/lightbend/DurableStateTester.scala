/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.state.scaladsl.Effect
import akka.serialization.jackson.JsonSerializable

import scala.concurrent.duration.DurationInt

object DurableStateCounter {
  sealed trait Command extends JsonSerializable
  final case class Increase(amount: Int, replyTo: ActorRef[Increased]) extends Command

  final case class GetState(replyTo: ActorRef[State]) extends Command

  final case class Increased(newValue: Int) extends JsonSerializable

  final case class State(value: Int) extends JsonSerializable
  def apply(id: String): Behavior[Command] =
    DurableStateBehavior[Command, State](
      PersistenceId("DSCounter", id),
      State(0),
      {
        case (state, Increase(amount, replyTo)) =>
          Effect.persist(State(state.value + amount)).thenReply(replyTo)(newState => Increased(newState.value))
        case (state, GetState(replyTo)) =>
          Effect.reply(replyTo)(state)
      })
}

object DurableStateTester {

  def apply(whenDone: ActorRef[String]): Behavior[AnyRef] = Behaviors.setup { context =>
    Behaviors.withTimers { timers =>
      timers.startSingleTimer("Timeout", 10.seconds)

      var durableActor = context.spawn(DurableStateCounter("one"), "DurableOne")
      context.watchWith(durableActor, "DurableOneStopped")

      def messageOrTimeout(step: String)(partial: PartialFunction[AnyRef, Behavior[AnyRef]]): Behavior[AnyRef] = {
        context.log.info("On {}", step)
        Behaviors.receiveMessage(message =>
          partial.orElse[AnyRef, Behavior[AnyRef]] {
            case "Timeout" =>
              context.log.error(s"Durable state checks timed out in {}", step)
              System.exit(1)
              Behaviors.same

            case other =>
              context.log.warn("Unexpected message in {}: {}", step, other)
              Behaviors.same
          }(message))
      }

      durableActor ! DurableStateCounter.Increase(1, context.self)

      def step1() = messageOrTimeout("step1") { case DurableStateCounter.Increased(1) =>
        // write works
        context.stop(durableActor)
        step2()
      }

      def step2() = messageOrTimeout("step2") { case "DurableOneStopped" =>
        durableActor = context.spawn(DurableStateCounter("one"), "DurableOneIncarnation2")
        durableActor ! DurableStateCounter.GetState(context.self)
        step3()
      }

      def step3() = messageOrTimeout("step3") { case DurableStateCounter.State(1) =>
        whenDone ! "Durable State works"
        Behaviors.stopped
      }

      step1()
    }
  }

}
