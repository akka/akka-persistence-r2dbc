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
 * Java API: Tool for deleting durable state for a given list of `persistenceIds` without using `DurableStateBehavior`
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
final class DurableStateCleanup private (delegate: scaladsl.DurableStateCleanup) {

  def this(systemProvider: ClassicActorSystemProvider, configPath: String) =
    this(new scaladsl.DurableStateCleanup(systemProvider, configPath))

  def this(systemProvider: ClassicActorSystemProvider) =
    this(systemProvider, "akka.persistence.r2dbc.cleanup")

  /**
   * Delete the state related to one single `persistenceId`.
   */
  def deleteState(persistenceId: String, neverUsePersistenceIdAgain: Boolean): CompletionStage[Done] = {
    delegate.deleteState(persistenceId, neverUsePersistenceIdAgain).toJava
  }

  /**
   * Delete all states related to the given list of `persistenceIds`.
   */
  def deleteStates(persistenceIds: JList[String], neverUsePersistenceIdAgain: Boolean): CompletionStage[Done] = {
    delegate.deleteStates(persistenceIds.asScala.toVector, neverUsePersistenceIdAgain).toJava
  }

}
