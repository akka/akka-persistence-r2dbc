/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import java.util.UUID

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Random
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.typed.PersistenceId
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

/**
 * App to store many events as test data. Useful when analyzing query plans and performance.
 *
 * Before running this you must:
 *   - Create the table (and index) `test_journal` corresponding to `event_journal`
 *   - Change logback-test.xml to only use STDOUT (no capturing)
 */
object TestDataGenerator {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory
      .parseString("""
      akka.persistence.r2dbc {
        journal.table = test_journal
      }
      """)
      .withFallback(TestConfig.config)
    ActorSystem(Generator(), "GenerateData", config)
  }

  object Generator {
    sealed trait Command
    case object Next extends Command

    val parallelism = 10
    val numEntities = 10000
    val numEvents = 100000

    val rnd = new Random(seed = 0)
    implicit val askTimeout: Timeout = 10.seconds

    def apply(): Behavior[Command] = {
      Behaviors.setup { context =>
        (0 until parallelism).foreach(_ => context.self ! Next)
        behavior(numEvents, Map.empty)
      }
    }

    private def behavior(remaining: Int, entities: Map[Int, ActorRef[Persister.Command]]): Behavior[Command] = {
      Behaviors.receive { case (context, Next) =>
        if (remaining == 0)
          Behaviors.stopped
        else {
          if (remaining % 1000 == 0)
            context.log.info("Remaining [{}]", remaining)

          val i = rnd.nextInt(numEntities)
          val ref = entities.getOrElse(
            i, {
              val entityId = UUID.randomUUID().toString
              val pid = PersistenceId("test", entityId)
              context.spawn(Persister(pid), entityId)
            })

          val event = s"evt-${UUID.randomUUID()}"
          context.ask[Persister.PersistWithAck, Done](ref, Persister.PersistWithAck(event, _)) {
            case Success(_)   => Next
            case Failure(exc) => throw exc
          }

          behavior(remaining - 1, entities.updated(i, ref))
        }
      }
    }
  }

}
