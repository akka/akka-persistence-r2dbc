/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
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
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * App to store many events as test data. Useful when analyzing query plans and performance.
 *
 * Before running this you must:
 *   - Create the table (and index) `test_journal` corresponding to `event_journal`
 *   - Change logback-test.xml to only use STDOUT (no capturing)
 *
 * You can define main arg `eventsBySlices` to run the query at the same time.
 */
object TestDataGenerator {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory
      .parseString("""
      akka.persistence.r2dbc {
        journal.table = test_journal
      }
      """)
      .withFallback(TestConfig.config)

    ActorSystem[Nothing](Main(query = args.contains("eventsBySlices")), "GenerateData", config)
  }

  object Main {
    def apply(query: Boolean): Behavior[Nothing] =
      Behaviors.setup[Nothing] { ctx =>
        if (query)
          startEventsByQuery(ctx.system)

        ctx.spawn(Generator(), "generator")
        Behaviors.same
      }

    def startEventsByQuery(system: ActorSystem[_]): Unit = {
      import system.executionContext
      implicit val sys = system

      val query = PersistenceQuery(system)
        .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

      val done = query
        .eventsBySlices[String](Generator.EntityType, 0, 255, NoOffset)
        .runFold((0L, 0L)) { case ((count, backtrackingCount), env) =>
          val (newCount, newBacktrackingCount) =
            if (env.eventOption.isDefined)
              (count + 1, backtrackingCount)
            else
              (count, backtrackingCount + 1)

          if ((newCount + newBacktrackingCount) % 100 == 0) {
            log.info(
              "eventsBySlices received [{}] events, and [{}] backtracking events",
              newCount,
              newBacktrackingCount)
          }

          (newCount, newBacktrackingCount)
        }

      done.onComplete {
        case Success((count, backtrackingCount)) =>
          log.info(
            "eventsBySlices completed and received total [{}] events, and [{}] backtracking events",
            count,
            backtrackingCount)
        case Failure(exc) =>
          log.error("eventsBySlices failed", exc)
          system.terminate()
      }
    }
  }

  object Generator {
    sealed trait Command
    case object Next extends Command

    val EntityType = "test"

    val parallelism = 10
    val numEntities = 10000
    val numEvents = 10000

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
              val pid = PersistenceId(EntityType, entityId)
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
