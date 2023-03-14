/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

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
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.scaladsl.DurableStateStoreBySliceQuery
import akka.persistence.r2dbc.TestActors.DurableStatePersister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.typed.PersistenceId
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * App to store many durable state test data. Useful when analyzing query plans and performance.
 *
 * Before running this you must:
 *   - Create the table (and index) `test_durable_state` corresponding to `durable_state`
 *   - Change logback-test.xml to only use STDOUT (no capturing)
 *
 * You can define main arg `changesBySlices` to run the query at the same time.
 */
object TestDataGenerator {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory
      .parseString("""
      akka.persistence.r2dbc {
        state.table = test_durable_state
      }
      """)
      .withFallback(TestConfig.config)

    ActorSystem[Nothing](Main(query = args.contains("changesBySlices")), "GenerateData", config)
  }

  object Main {
    def apply(query: Boolean): Behavior[Nothing] =
      Behaviors.setup[Nothing] { ctx =>
        if (query)
          startChangesByQuery(ctx.system)

        ctx.spawn(Generator(), "generator")
        Behaviors.same
      }

    def startChangesByQuery(system: ActorSystem[_]): Unit = {
      import system.executionContext
      implicit val sys = system

      val query = DurableStateStoreRegistry(system)
        .durableStateStoreFor[DurableStateStoreBySliceQuery[String]](R2dbcDurableStateStore.Identifier)

      val done = query
        .changesBySlices(Generator.EntityType, 0, 255, NoOffset)
        .collectType[UpdatedDurableState[String]]
        .runFold(0L) { case (count, _) =>
          val newCount = count + 1

          if (newCount % 100 == 0)
            log.info("changesBySlices received [{}] changes", newCount)

          newCount
        }

      done.onComplete {
        case Success(count) =>
          log.info("changesBySlices completed and received total [{}] changes", count)
        case Failure(exc) =>
          log.error("changesBySlices failed", exc)
          system.terminate()
      }
    }
  }

  object Generator {
    sealed trait Command
    case object Next extends Command

    val EntityType = "test"

    val parallelism = 5
    val numEntities = 100
    val numUpdates = 10000

    val rnd = new Random(seed = 0)
    implicit val askTimeout: Timeout = 20.seconds

    def apply(): Behavior[Command] = {
      Behaviors.setup { context =>
        (0 until parallelism).foreach(_ => context.self ! Next)
        behavior(numUpdates, Map.empty)
      }
    }

    private def behavior(
        remaining: Int,
        entities: Map[Int, ActorRef[DurableStatePersister.Command]]): Behavior[Command] = {
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
              context.spawn(DurableStatePersister(pid), entityId)
            })

          val value = s"value-${UUID.randomUUID()}"
          context.ask[DurableStatePersister.PersistWithAck, Done](ref, DurableStatePersister.PersistWithAck(value, _)) {
            case Success(_)   => Next
            case Failure(exc) => throw exc
          }

          behavior(remaining - 1, entities.updated(i, ref))
        }
      }
    }
  }

}
