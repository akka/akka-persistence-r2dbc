/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.serialization.jackson.JsonSerializable
import com.fasterxml.jackson.annotation.JsonCreator

import scala.concurrent.duration.DurationInt

object EventSourcedCounter {
  sealed trait Command extends JsonSerializable

  final case class Increase(amount: Int, replyTo: ActorRef[StatusReply[Increased]]) extends Command
  final case class GetValue @JsonCreator() (replyTo: ActorRef[StatusReply[GetValueResponse]]) extends Command
  final case class GetValueResponse(value: Int)

  sealed trait Event extends JsonSerializable

  final case class Increased @JsonCreator() (amount: Int) extends Event

  final case class State @JsonCreator() (value: Int) extends JsonSerializable

  def apply(id: String): Behavior[Command] = EventSourcedBehavior[Command, Event, State](
    PersistenceId("EventSourcedHelloWorld", id),
    State(0),
    { (state, command) =>
      command match {
        case Increase(increment, replyTo) =>
          val increased = Increased(increment)
          Effect.persist(increased).thenReply(replyTo)(_ => StatusReply.success(increased))
        case GetValue(replyTo) =>
          Effect.reply(replyTo)(StatusReply.success(GetValueResponse(state.value)))
      }
    },
    { (_, event) =>
      event match {
        case Increased(newGreeting) => State(newGreeting)
      }
    }).snapshotWhen((_, _, seqNr) => seqNr % 2 == 0)
}

object EsbTester {

  object EsbStopped

  def apply(whenDone: ActorRef[String]): Behavior[AnyRef] = Behaviors.setup { context =>
    Behaviors.withTimers { timers =>

      timers.startSingleTimer("Timeout", 10.seconds)

      var eventSourcedHelloWorld = context.spawn(EventSourcedCounter("one"), "EsbOne")
      context.watchWith(eventSourcedHelloWorld, EsbStopped)
      eventSourcedHelloWorld ! EventSourcedCounter.Increase(1, context.self)

      def messageOrTimeout(step: String)(partial: PartialFunction[AnyRef, Behavior[AnyRef]]): Behavior[AnyRef] = {
        context.log.info("On {}", step)
        Behaviors.receiveMessage(message =>
          partial.orElse[AnyRef, Behavior[AnyRef]] {
            case "Timeout" =>
              context.log.error(s"ESB checks timed out in {}", step)
              System.exit(1)
              Behaviors.same

            case other =>
              context.log.warn("Unexpected message in {}: {}", step, other)
              Behaviors.same
          }(message))
      }

      def step1() = messageOrTimeout("step1") { case StatusReply.Success(EventSourcedCounter.Increased(1)) =>
        eventSourcedHelloWorld ! EventSourcedCounter.Increase(2, context.self)
        step2()
      }

      def step2() =
        messageOrTimeout("step2") { case StatusReply.Success(EventSourcedCounter.Increased(2)) =>
          // triggers snapshot
          eventSourcedHelloWorld ! EventSourcedCounter.Increase(2, context.self)
          step3()
        }

      def step3() =
        messageOrTimeout("step3") { case StatusReply.Success(EventSourcedCounter.Increased(2)) =>
          eventSourcedHelloWorld ! EventSourcedCounter.GetValue(context.self)
          step4()
        }

      def step4() = messageOrTimeout("step4") { case StatusReply.Success(EventSourcedCounter.GetValueResponse(2)) =>
        context.stop(eventSourcedHelloWorld)
        step5()
      }

      def step5() = messageOrTimeout("step5") { case EsbStopped =>
        // start anew to trigger replay
        eventSourcedHelloWorld = context.spawn(EventSourcedCounter("one"), "EsbOneIncarnation2")
        eventSourcedHelloWorld ! EventSourcedCounter.GetValue(context.self)
        step6()

      }

      def step6() = messageOrTimeout("step6") { case StatusReply.Success(EventSourcedCounter.GetValueResponse(2)) =>
        // replay was fine
        whenDone ! "ESB works"
        Behaviors.stopped
      }

      step1()
    }
  }
}
