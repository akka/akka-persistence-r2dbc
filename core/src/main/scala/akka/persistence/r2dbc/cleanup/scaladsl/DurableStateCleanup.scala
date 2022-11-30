/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.scaladsl

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao
import org.slf4j.LoggerFactory

/**
 * Scala API: Tool for deleting durable state for a given list of `persistenceIds` without using `DurableStateBehavior`
 * actors. It's important that the actors with corresponding persistenceId are not running at the same time as using the
 * tool.
 *
 * If `neverUsePersistenceIdAgain` is `true` then the highest used revision number is deleted and the `persistenceId`
 * should not be used again, since it would be confusing to reuse the same revision numbers for new state changes.
 *
 * When a list of `persistenceIds` are given they are deleted sequentially in the order of the list. It's possible to
 * parallelize the deletes by running several cleanup operations at the same time operating on different sets of
 * `persistenceIds`.
 */
@ApiMayChange
final class DurableStateCleanup(systemProvider: ClassicActorSystemProvider, configPath: String) {

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.r2dbc.cleanup")

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] implicit val system: ActorSystem[_] = {
    import akka.actor.typed.scaladsl.adapter._
    systemProvider.classicSystem.toTyped
  }

  import system.executionContext

  private val log = LoggerFactory.getLogger(classOf[DurableStateCleanup])

  private val sharedConfigPath = configPath.replaceAll("""\.cleanup$""", "")
  private val settings = R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))

  private val connectionFactory =
    ConnectionFactoryProvider(system).connectionFactoryFor(sharedConfigPath + ".connection-factory")
  private val stateDao = new DurableStateDao(settings, connectionFactory)

  /**
   * Delete the state related to one single `persistenceId`.
   */
  def deleteState(persistenceId: String, neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    if (neverUsePersistenceIdAgain)
      stateDao.deleteState(persistenceId, revision = 0L) // hard delete without revision check
    else {
      stateDao.readState(persistenceId).flatMap {
        case None    => Future.successful(Done) // already deleted
        case Some(s) => stateDao.deleteState(persistenceId, s.revision + 1)
      }
    }
  }

  /**
   * Delete all states related to the given list of `persistenceIds`.
   */
  def deleteStates(persistenceIds: immutable.Seq[String], neverUsePersistenceIdAgain: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteStates", pid => deleteState(pid, neverUsePersistenceIdAgain))
  }

  private def foreach(
      persistenceIds: immutable.Seq[String],
      operationName: String,
      pidOperation: String => Future[Done]): Future[Done] = {
    val size = persistenceIds.size
    log.info("Cleanup started {} of [{}] persistenceId.", operationName, size)

    def loop(remaining: List[String], n: Int): Future[Done] = {
      remaining match {
        case Nil => Future.successful(Done)
        case pid :: tail =>
          pidOperation(pid).flatMap { _ =>
            if (n % settings.cleanupSettings.logProgressEvery == 0)
              log.infoN("Cleanup {} [{}] of [{}].", operationName, n, size)
            loop(tail, n + 1)
          }
      }
    }

    val result = loop(persistenceIds.toList, n = 1)

    result.onComplete {
      case Success(_) =>
        log.info2("Cleanup completed {} of [{}] persistenceId.", operationName, size)
      case Failure(e) =>
        log.error(s"Cleanup {$operationName} failed.", e)
    }

    result
  }

}
