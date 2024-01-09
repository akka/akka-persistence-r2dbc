/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.sql

import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import io.r2dbc.spi.{ Row, Statement }

import java.time.Instant
import scala.collection.immutable

trait BaseJournalSql {
  def bindForDeleteEventsSql(stmt: Statement, persistenceId: String, from: Long, to: Long): Statement

  def bindForInsertDeleteMarkerSql(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      deleteMarkerSeqNr: Long): Statement

  val selectHighestSequenceNrSql: String
  val selectLowestSequenceNrSql: String
  val insertDeleteMarkerSql: String
  val deleteEventsSql: String

  def bindSelectHighestSequenceNrSql(stmt: Statement, persistenceId: String, fromSequenceNr: Long): Statement

  def bindSelectLowestSequenceNrSql(stmt: Statement, persistenceId: String): Statement

  def parseInsertForWriteEvent(row: Row): Instant

  def deleteEventsBySliceBeforeTimestamp(
      createStatement: String => Statement,
      slice: Int,
      entityType: String,
      timestamp: Instant): Statement

  def deleteEventsByPersistenceIdBeforeTimestamp(
      createStatement: String => Statement,
      persistenceId: String,
      timestamp: Instant): Statement

  def bindInsertForWriteEvent(
      stmt: Statement,
      write: SerializedJournalRow,
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): Statement

  def insertSql(useTimestampFromDb: Boolean): String
}
