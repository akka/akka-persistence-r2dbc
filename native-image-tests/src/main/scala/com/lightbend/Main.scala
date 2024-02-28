package com.lightbend

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration.DurationInt

object RootBehavior {
  def apply(): Behavior[AnyRef] = Behaviors.setup { context =>
    Behaviors.withTimers { timers =>
      timers.startSingleTimer("Timeout", 30.seconds)
      context.spawn(EsbTester(context.self), "ESBTester")
      context.spawn(DurableStateTester(context.self), "DurableStateTester")

      var awaitedOks = Set("ESB works", "Durable State works")

      Behaviors.receiveMessage {
        case "Timeout" =>
          context.log.error("Suite of checks timed out, missing awaitedOks: {}", awaitedOks)
          System.exit(1)
          Behaviors.same

        case string: String =>
          awaitedOks -= string
          if (awaitedOks.isEmpty) {
            context.log.info("All checks ok, shutting down")
            Behaviors.stopped
          } else {
            context.log.info("Continuing, awaitedOks not empty: {}", awaitedOks)
            Behaviors.same
          }
        case other =>
          context.log.warn("Unexpected message: {}", other)
          Behaviors.same
      }
    }
  }
}

object Main extends App {

  ActorSystem(RootBehavior(), "R2dbcTester")

}
