/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.cleanup.javadsl

import java.util.concurrent.CompletionStage
import java.util.{ List => JList }

import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.persistence.r2dbc.cleanup.scaladsl

/**
 * Java API: Tool for deleting durable state for a given list of `persistenceIds` without using `DurableStateBehavior`
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
final class DurableStateCleanup private (delegate: scaladsl.DurableStateCleanup) {

  def this(systemProvider: ClassicActorSystemProvider, configPath: String) =
    this(new scaladsl.DurableStateCleanup(systemProvider, configPath))

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.r2dbc.cleanup")

  /**
   * Delete the state related to one single `persistenceId`.
   */
  def deleteState(persistenceId: String, resetRevisionNumber: Boolean): CompletionStage[Done] = {
    delegate.deleteState(persistenceId, resetRevisionNumber).asJava
  }

  /**
   * Delete all states related to the given list of `persistenceIds`.
   */
  def deleteStates(persistenceIds: JList[String], resetRevisionNumber: Boolean): CompletionStage[Done] = {
    delegate.deleteStates(persistenceIds.asScala.toVector, resetRevisionNumber).asJava
  }

}
