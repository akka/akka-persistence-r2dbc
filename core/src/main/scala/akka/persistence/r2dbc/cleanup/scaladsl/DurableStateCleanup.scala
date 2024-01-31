/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.scaladsl

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import org.slf4j.LoggerFactory

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * Scala API: Tool for deleting durable state for a given list of `persistenceIds` without using `DurableStateBehavior`
 * actors. It's important that the actors with corresponding persistenceId are not running at the same time as using the
 * tool.
 *
 * If `resetRevisionNumber` is `true` then the creating entity with the same `persistenceId` will start from 0.
 * Otherwise it will continue from the latest highest used revision number.
 *
 * WARNING: reusing the same `persistenceId` after resetting the revision number should be avoided, since it might be
 * confusing to reuse the same revision numbers for new state changes.
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

  private val executorProvider =
    new R2dbcExecutorProvider(settings, sharedConfigPath + ".connection-factory", LoggerFactory.getLogger(getClass))
  private val stateDao = settings.connectionFactorySettings.dialect.createDurableStateDao(settings, executorProvider)

  /**
   * Delete the state related to one single `persistenceId`.
   */
  def deleteState(persistenceId: String, resetRevisionNumber: Boolean): Future[Done] = {
    if (resetRevisionNumber)
      stateDao
        .deleteState(persistenceId, revision = 0L, changeEvent = None) // hard delete without revision check
        .map(_ => Done)(ExecutionContexts.parasitic)
    else {
      stateDao.readState(persistenceId).flatMap {
        case None =>
          Future.successful(Done) // already deleted
        case Some(s) =>
          stateDao
            .deleteState(persistenceId, s.revision + 1, changeEvent = None)
            .map(_ => Done)(ExecutionContexts.parasitic)
      }
    }
  }

  /**
   * Delete all states related to the given list of `persistenceIds`.
   */
  def deleteStates(persistenceIds: immutable.Seq[String], resetRevisionNumber: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteStates", pid => deleteState(pid, resetRevisionNumber))
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
