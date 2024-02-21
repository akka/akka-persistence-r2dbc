package com.lightbend

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object RootBehavior {
  def apply(): Behavior[AnyRef] = Behaviors.setup { context =>
    context.spawn(EsbTester(context.self), "ESBTester")

    var awaitedOks = Set("ESB works")

    Behaviors.receiveMessage {
      case string: String =>
        awaitedOks -= string
        if (awaitedOks.isEmpty) {
          context.log.info("All checks ok, shutting down")
          Behaviors.stopped
        } else {
          Behaviors.same
        }
      case other =>
        context.log.warn("Unexpected message: {}", other)
        Behaviors.same
    }
  }
}

object Main extends App {

  ActorSystem(RootBehavior(), "R2dbcTester")

}
