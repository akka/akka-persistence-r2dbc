/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.javadsl

import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.{ List => JList }

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.persistence.r2dbc.cleanup.scaladsl

/**
 * Java API: Tool for deleting events and/or snapshots for a given list of `persistenceIds` without using persistent
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
final class EventSourcedCleanup private (delegate: scaladsl.EventSourcedCleanup) {

  def this(systemProvider: ClassicActorSystemProvider, configPath: String) =
    this(new scaladsl.EventSourcedCleanup(systemProvider, configPath))

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.r2dbc.cleanup")

  /**
   * Delete all events before a sequenceNr for the given persistence id. Snapshots are not deleted.
   *
   * @param persistenceId
   *   the persistence id to delete for
   * @param toSequenceNr
   *   sequence nr (inclusive) to delete up to
   */
  def deleteEventsTo(persistenceId: String, toSequenceNr: Long): CompletionStage[Done] =
    delegate.deleteEventsTo(persistenceId, toSequenceNr).asJava

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceId, resetSequenceNumber).asJava

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceIds.asScala.toVector, resetSequenceNumber).asJava

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
  def deleteEventsBefore(persistenceId: String, timestamp: Instant): CompletionStage[Done] =
    delegate.deleteEventsBefore(persistenceId, timestamp).asJava

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
  def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): CompletionStage[Done] =
    delegate.deleteEventsBefore(entityType, slice, timestamp).asJava

  /**
   * Delete snapshots related to one single `persistenceId`. Events are not deleted.
   */
  def deleteSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.deleteSnapshot(persistenceId).asJava

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   */
  def deleteSnapshots(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.deleteSnapshots(persistenceIds.asScala.toVector).asJava

  /**
   * Deletes all events for the given persistence id from before the snapshot. The snapshot is not deleted. The event
   * with the same sequence number as the remaining snapshot is deleted.
   */
  def cleanupBeforeSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceId).asJava

  /**
   * See single persistenceId overload for what is done for each persistence id
   */
  def cleanupBeforeSnapshot(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceIds.asScala.toVector).asJava

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceId, resetSequenceNumber).asJava

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceIds.asScala.toVector, resetSequenceNumber).asJava

}
