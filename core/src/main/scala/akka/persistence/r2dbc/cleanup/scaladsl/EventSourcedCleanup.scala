/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.scaladsl

import java.time.Instant

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
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * Scala API: Tool for deleting events and/or snapshots for a given list of `persistenceIds` without using persistent
 * actors.
 *
 * When running an operation with `EventSourcedCleanup` that deletes all events for a persistence id, the actor with
 * that persistence id must not be running! If the actor is restarted it would in that case be recovered to the wrong
 * state since the stored events have been deleted. Delete events before snapshot can still be used while the actor is
 * running.
 *
 * If `resetSequenceNumber` is `true` then the creating entity with the same `persistenceId` will start from 0.
 * Otherwise it will continue from the latest highest used sequence number.
 *
 * WARNING: reusing the same `persistenceId` after resetting the sequence number should be avoided, since it might be
 * confusing to reuse the same sequence number for new events.
 *
 * When a list of `persistenceIds` are given they are deleted sequentially in the order of the list. It's possible to
 * parallelize the deletes by running several cleanup operations at the same time operating on different sets of
 * `persistenceIds`.
 */
@ApiMayChange
final class EventSourcedCleanup(systemProvider: ClassicActorSystemProvider, configPath: String) {

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

  private val log = LoggerFactory.getLogger(classOf[EventSourcedCleanup])

  private val sharedConfigPath = configPath.replaceAll("""\.cleanup$""", "")
  private val settings = R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))

  private val executorProvider =
    new R2dbcExecutorProvider(
      system,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, system),
      settings,
      sharedConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))
  private val journalDao = settings.connectionFactorySettings.dialect.createJournalDao(executorProvider)
  private val snapshotDao = settings.connectionFactorySettings.dialect.createSnapshotDao(executorProvider)

  /**
   * Delete all events before a sequenceNr for the given persistence id. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param toSequenceNr
   *   sequence nr (inclusive) to delete up to
   */
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long): Future[Done] = {
    log.debug("deleteEventsTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteEventsTo(persistenceId, toSequenceNr, resetSequenceNumber = false).map(_ => Done)
  }

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceId: String, resetSequenceNumber: Boolean): Future[Done] = {
    journalDao
      .deleteEventsTo(persistenceId, toSequenceNr = Long.MaxValue, resetSequenceNumber)
      .map(_ => Done)
  }

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceIds: immutable.Seq[String], resetSequenceNumber: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAllEvents", pid => deleteAllEvents(pid, resetSequenceNumber))
  }

  /**
   * Delete events before a timestamp for the given persistence id. Snapshots are not deleted.
   *
   * This can be useful for `DurableStateBehavior` with change events, where the events are only used for the
   * Projections and not for the recovery of the `DurableStateBehavior` state. The timestamp may correspond to the
   * offset timestamp of the Projections, if events are not needed after all Projections have processed them.
   *
   * Be aware of that if all events of a persistenceId are removed the sequence number will start from 1 again if an
   * `EventSourcedBehavior` with the same persistenceId is used again.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param timestamp
   *   timestamp (exclusive) to delete up to
   */
  def deleteEventsBefore(persistenceId: String, timestamp: Instant): Future[Done] = {
    log.debug("deleteEventsBefore persistenceId [{}], timestamp [{}]", persistenceId, timestamp)
    journalDao.deleteEventsBefore(persistenceId, timestamp).map(_ => Done)
  }

  /**
   * Delete events before a timestamp for the given entityType and slice. Snapshots are not deleted.
   *
   * This can be useful for `DurableStateBehavior` with change events, where the events are only used for the
   * Projections and not for the recovery of the `DurableStateBehavior` state. The timestamp may correspond to the
   * offset timestamp of the Projections, if events are not needed after all Projections have processed them.
   *
   * Be aware of that if all events of a persistenceId are removed the sequence number will start from 1 again if an
   * `EventSourcedBehavior` with the same persistenceId is used again.
   *
   * @param entityType
   *   the entity type to delete for
   * @param slice
   *   the slice to delete for
   * @param timestamp
   *   timestamp (exclusive) to delete up to
   */
  def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): Future[Done] = {
    log.debug("deleteEventsBefore [{}], slice [{}] timestamp [{}]", entityType, slice, timestamp)
    journalDao.deleteEventsBefore(entityType, slice, timestamp).map(_ => Done)
  }

  /**
   * Delete snapshots related to one single `persistenceId`. Events are not deleted.
   */
  def deleteSnapshot(persistenceId: String): Future[Done] = {
    snapshotDao
      .delete(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = Long.MaxValue))
      .map(_ => Done)
  }

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   */
  def deleteSnapshots(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "deleteSnapshots", pid => deleteSnapshot(pid))
  }

  /**
   * Deletes all events for the given persistence id from before the snapshot. The snapshot is not deleted. The event
   * with the same sequence number as the remaining snapshot is deleted.
   */
  def cleanupBeforeSnapshot(persistenceId: String): Future[Done] = {
    snapshotDao.load(persistenceId, SnapshotSelectionCriteria.Latest).flatMap {
      case None => Future.successful(Done)
      case Some(snapshot) =>
        deleteEventsTo(persistenceId, snapshot.seqNr)
    }
  }

  /**
   * See single persistenceId overload for what is done for each persistence id
   */
  def cleanupBeforeSnapshot(persistenceIds: immutable.Seq[String]): Future[Done] = {
    foreach(persistenceIds, "cleanupBeforeSnapshot", pid => cleanupBeforeSnapshot(pid))
  }

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceId: String, resetSequenceNumber: Boolean): Future[Done] = {
    for {
      _ <- deleteAllEvents(persistenceId, resetSequenceNumber)
      _ <- deleteSnapshot(persistenceId)
    } yield Done
  }

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceIds: immutable.Seq[String], resetSequenceNumber: Boolean): Future[Done] = {
    foreach(persistenceIds, "deleteAll", pid => deleteAll(pid, resetSequenceNumber))
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
