/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.javadsl

import java.util.concurrent.CompletionStage
import java.util.{ List => JList }

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.persistence.r2dbc.cleanup.scaladsl

/**
 * Java API: Tool for deleting all events and/or snapshots for a given list of `persistenceIds` without using persistent
 * actors. It's important that the actors with corresponding `persistenceId` are not running at the same time as using
 * the tool.
 *
 * WARNING: deleting events is generally discouraged in event sourced systems.
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
    delegate.deleteEventsTo(persistenceId, toSequenceNr).toJava

  /**
   * Delete all events related to one single `persistenceId`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceId, resetSequenceNumber).toJava

  /**
   * Delete all events related to the given list of `persistenceIds`. Snapshots are not deleted.
   */
  def deleteAllEvents(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAllEvents(persistenceIds.asScala.toVector, resetSequenceNumber).toJava

  /**
   * Delete snapshots related to one single `persistenceId`. Events are not deleted.
   */
  def deleteSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.deleteSnapshot(persistenceId).toJava

  /**
   * Delete all snapshots related to the given list of `persistenceIds`. Events are not deleted.
   */
  def deleteSnapshots(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.deleteSnapshots(persistenceIds.asScala.toVector).toJava

  /**
   * Deletes all events for the given persistence id from before the snapshot. The snapshot is not deleted. The event
   * with the same sequence number as the remaining snapshot is deleted.
   */
  def cleanupBeforeSnapshot(persistenceId: String): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceId).toJava

  /**
   * See single persistenceId overload for what is done for each persistence id
   */
  def cleanupBeforeSnapshot(persistenceIds: JList[String]): CompletionStage[Done] =
    delegate.cleanupBeforeSnapshot(persistenceIds.asScala.toVector).toJava

  /**
   * Delete everything related to one single `persistenceId`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceId: String, resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceId, resetSequenceNumber).toJava

  /**
   * Delete everything related to the given list of `persistenceIds`. All events and snapshots are deleted.
   */
  def deleteAll(persistenceIds: JList[String], resetSequenceNumber: Boolean): CompletionStage[Done] =
    delegate.deleteAll(persistenceIds.asScala.toVector, resetSequenceNumber).toJava

}
