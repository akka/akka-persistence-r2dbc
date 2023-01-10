/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import scala.collection.immutable
import scala.concurrent.Future

import akka.Done
import io.r2dbc.spi.Statement

object DaoExtension {
  final case class Upsert[A](
      val persistenceId: String,
      val entityType: String,
      val slice: Int,
      val revision: Long,
      val value: A)

  final case class Delete(val persistenceId: String, val entityType: String, val slice: Int, revision: Long)

  final case class AdditionalColumn[A](name: String, bind: Upsert[A] => AnyRef)

  /**
   * To bind a column to `null` this can be returned from the `bind` function in [[AdditionalColumn]].
   */
  final case class BindNull(columnType: Class[_])

  /**
   * To not update a column this can be returned from the `bind` function in [[AdditionalColumn]].
   */
  final case object Skip
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
  def additionalColumns(entityType: String): immutable.IndexedSeq[DaoExtension.AdditionalColumn[_]] =
    Vector.empty

  /**
   * Override this method to perform additional processing in the same transaction as the Durable State upsert.
   */
  def processOnUpsert(upsert: DaoExtension.Upsert[AnyRef], session: R2dbcSession): Future[Done] =
    Future.successful(Done)

  /**
   * Override this method to perform additional processing in the same transaction as the Durable State delete.
   */
  def processOnDelete(delete: DaoExtension.Delete, session: R2dbcSession): Future[Done] =
    Future.successful(Done)

}

// FIXME move R2dbcSession from akka-projection-r2dbc
trait R2dbcSession {
  def createStatement(sql: String): Statement
  def updateOne(statement: Statement): Future[Long]
}
