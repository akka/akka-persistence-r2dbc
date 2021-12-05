/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.javadsl

import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.{ List => JList }
import java.util.function.{ Function => JFunction }

import akka.Done

object DaoExtension {
  final class Upsert[A](
      val persistenceId: String,
      val entityType: String,
      val slice: Int,
      val revision: Long,
      val value: A)

  final class Delete(val persistenceId: String, val entityType: String, val slice: Int, revision: Long)

  final class AdditionalColumn[A](name: String, bind: JFunction[Upsert[A], AnyRef])

  /**
   * To bind a column to `null` this can be returned from the `bind` function in [[AdditionalColumn]].
   */
  final class BindNull(columnType: Class[_])
}

/**
 * A `DaoExtension` is a way to define additional processing for Durable State upserts or deletes.
 *
 * Implementation of this class can be registered in configuration property
 * `akka.persistence.r2dbc.state.dao-extension`. The implementation may optionally define an ActorSystem constructor
 * parameter.
 */
abstract class DaoExtension {

  /**
   * Override this method to use a different table for a specific `entityType`. Return `""` to use (default) configured
   * table name.
   */
  def tableName(entityType: String): String = ""

  /**
   * Override this method to define additional columns that will be updated together with the Durable State upsert
   * (insert/update). Such columns can be useful for queries on other fields than the primary key.
   *
   * When defining additional columns you have to amend the table definition to match those additional columns, and
   * typically add a secondary index on the columns. You can use a different table for a specific `entityType` by
   * defining the table name with [[DaoExtension.tableName]].
   */
  def additionalColumns(entityType: String): JList[DaoExtension.AdditionalColumn] =
    Collections.emptyList()

  /**
   * Override this method to perform additional processing in the same transaction as the Durable State upsert.
   */
  def processOnUpsert(upsert: DaoExtension.Upsert[AnyRef], session: R2dbcSession): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

  /**
   * Override this method to perform additional processing in the same transaction as the Durable State delete.
   */
  def processOnDelete(delete: DaoExtension.Delete, session: R2dbcSession): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

}

// FIXME move R2dbcSession from akka-projection-r2dbc
final class R2dbcSession
