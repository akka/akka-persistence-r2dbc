/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.lang
import java.time.Instant
import java.util

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import io.r2dbc.spi.Connection
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.NoOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.AdditionalColumnFactory
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.ChangeHandlerFactory
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.r2dbc.state.ChangeHandlerException
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.state.scaladsl.ChangeHandler
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresDurableStateDao {

  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresDurableStateDao])

  private[r2dbc] final case class EvaluatedAdditionalColumnBindings(
      additionalColumn: AdditionalColumn[_, _],
      binding: AdditionalColumn.Binding[_])

  val FutureDone: Future[Done] = Future.successful(Done)
  val FutureInstantNone: Future[Option[Instant]] = Future.successful(None)
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class PostgresDurableStateDao(executorProvider: R2dbcExecutorProvider, dialect: Dialect)
    extends DurableStateDao {
  protected val settings: R2dbcSettings = executorProvider.settings
  protected val system: ActorSystem[_] = executorProvider.system
  implicit protected val ec: ExecutionContext = executorProvider.ec
  import DurableStateDao._
  import PostgresDurableStateDao._
  import settings.codecSettings.DurableStateImplicits._
  protected def log: Logger = PostgresDurableStateDao.log

  private val persistenceExt = Persistence(system)

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  protected def shouldInsert(state: SerializedStateRow): Future[Boolean] = Future.successful(state.revision == 1)

  // used for change events
  private lazy val journalDao: JournalDao = dialect.createJournalDao(executorProvider)

  private lazy val additionalColumns: Map[String, immutable.IndexedSeq[AdditionalColumn[Any, Any]]] = {
    settings.durableStateAdditionalColumnClasses.map { case (entityType, columnClasses) =>
      val instances = columnClasses.map(fqcn => AdditionalColumnFactory.create(system, fqcn))
      entityType -> instances
    }
  }

  private lazy val changeHandlers: Map[String, ChangeHandler[Any]] = {
    settings.durableStateChangeHandlerClasses.map { case (entityType, fqcn) =>
      val handler = ChangeHandlerFactory.create(system, fqcn)
      entityType -> handler
    }
  }

  private def selectStateSql(slice: Int, entityType: String): String =
    sqlCache.get(slice, s"selectStateSql-$entityType") {
      val stateTable = settings.getDurableStateTableWithSchema(entityType, slice)
      sql"""
      SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
      FROM $stateTable WHERE persistence_id = ?"""
    }

  protected def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String =
    sqlCache.get(minSlice, s"selectBucketsSql-$entityType-$minSlice-$maxSlice") {
      val stateTable = settings.getDurableStateTableWithSchema(entityType, minSlice)
      sql"""
       SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
       FROM $stateTable
       WHERE entity_type = ?
       AND ${sliceCondition(minSlice, maxSlice)}
       AND db_timestamp >= ? AND db_timestamp <= ?
       GROUP BY bucket ORDER BY bucket LIMIT ?
       """
    }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  protected def insertStateSql(
      slice: Int,
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    def createSql = {
      val stateTable = settings.getDurableStateTableWithSchema(entityType, slice)
      val additionalCols = additionalInsertColumns(additionalBindings)
      val additionalParams = additionalInsertParameters(additionalBindings)
      sql"""
      INSERT INTO $stateTable
      (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?$additionalParams, CURRENT_TIMESTAMP)"""
    }

    if (additionalBindings.isEmpty)
      sqlCache.get(slice, s"insertStateSql-$entityType")(createSql)
    else
      createSql // no cache
  }

  protected def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(c, AdditionalColumn.BindNull) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  protected def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(_, _: AdditionalColumn.BindValue[_]) |
            EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindNull) =>
          strB.append(", ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  protected def updateStateSql(
      slice: Int,
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings],
      currentTimestamp: String = "CURRENT_TIMESTAMP"): String = {
    def createSql = {
      val stateTable = settings.getDurableStateTableWithSchema(entityType, slice)

      val timestamp =
        if (settings.dbTimestampMonotonicIncreasing)
          currentTimestamp
        else
          "GREATEST(CURRENT_TIMESTAMP, " +
          s"(SELECT db_timestamp + '1 microsecond'::interval FROM $stateTable WHERE persistence_id = ? AND revision = ?))"

      val revisionCondition =
        if (settings.durableStateAssertSingleWriter) " AND revision = ?"
        else ""

      val tags = if (updateTags) ", tags = ?" else ""

      val additionalParams = additionalUpdateParameters(additionalBindings)
      sql"""
      UPDATE $stateTable
      SET revision = ?, state_ser_id = ?, state_ser_manifest = ?, state_payload = ?$tags$additionalParams, db_timestamp = $timestamp
      WHERE persistence_id = ?
      $revisionCondition"""
    }

    if (additionalBindings.isEmpty)
      // timestamp param doesn't have to be part of cache key because it's just different for different dialects
      sqlCache.get(slice, s"updateStateSql-$entityType-$updateTags")(createSql)
    else
      createSql // no cache
  }

  private def additionalUpdateParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  private def hardDeleteStateSql(entityType: String, slice: Int): String = {
    sqlCache.get(slice, s"hardDeleteStateSql-$entityType") {
      val stateTable = settings.getDurableStateTableWithSchema(entityType, slice)
      sql"DELETE from $stateTable WHERE persistence_id = ?"
    }
  }

  private val currentDbTimestampSql =
    sql"SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def allPersistenceIdsSql(table: String): String = {
    // not worth caching
    sql"SELECT persistence_id from $table ORDER BY persistence_id LIMIT ?"
  }

  protected def persistenceIdsForEntityTypeSql(table: String): String = {
    // not worth caching
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"
  }

  protected def allPersistenceIdsAfterSql(table: String): String = {
    // not worth caching
    sql"SELECT persistence_id from $table WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"
  }

  protected def bindPersistenceIdsForEntityTypeAfterSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      after: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityType + likeStmtPostfix)
      .bind(1, after)
      .bind(2, limit)
  }

  protected def persistenceIdsForEntityTypeAfterSql(table: String): String = {
    // not worth caching
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"
  }

  protected def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis} milliseconds'"
    else ""

  protected def stateBySlicesRangeSql(
      entityType: String,
      maxDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    // not caching, too many combinations

    val stateTable = settings.getDurableStateTableWithSchema(entityType, minSlice)

    def maxDbTimestampParamCondition =
      if (maxDbTimestampParam) s"AND db_timestamp < ?" else ""

    val behindCurrentTimeIntervalCondition = behindCurrentTimeIntervalConditionFor(behindCurrentTime)

    val selectColumns =
      if (backtracking)
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id "
      else
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
      $selectColumns
      FROM $stateTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      ORDER BY db_timestamp, revision
      LIMIT ?"""
  }

  override def readState(persistenceId: String): Future[Option[SerializedStateRow]] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor.selectOne(s"select [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectStateSql(slice, entityType))
          .bind(0, persistenceId),
      row =>
        SerializedStateRow(
          persistenceId = persistenceId,
          revision = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
          dbTimestamp = row.getTimestamp("db_timestamp"),
          readDbTimestamp = Instant.EPOCH, // not needed here
          payload = getPayload(row),
          serId = row.get[Integer]("state_ser_id", classOf[Integer]),
          serManifest = row.get("state_ser_manifest", classOf[String]),
          tags = Set.empty // tags not fetched in queries (yet)
        ))
  }

  private def getPayload(row: Row): Option[Array[Byte]] = {
    val serId = row.get("state_ser_id", classOf[Integer])
    val rowPayload = row.getPayload("state_payload")
    if (serId == 0 && (rowPayload == null || util.Arrays.equals(durableStatePayloadCodec.nonePayload, rowPayload)))
      None // delete marker
    else
      Option(rowPayload)
  }

  private def writeChangeEventAndCallChangeHandler(
      connection: Connection,
      updatedRows: Long,
      entityType: String,
      change: DurableStateChange[Any],
      changeEvent: Option[SerializedJournalRow]): Future[Option[Instant]] = {
    if (updatedRows == 1)
      for {
        changeEventTimestamp <- changeEvent
          .map(journalDao.writeEventInTx(_, connection).map(Some(_)))
          .getOrElse(FutureInstantNone)
        _ <- changeHandlers.get(entityType).map(processChange(_, connection, change)).getOrElse(FutureDone)
      } yield changeEventTimestamp
    else
      FutureInstantNone
  }

  protected def bindTimestampNow(stmt: Statement, getAndIncIndex: () => Int): Statement = stmt

  override def upsertState(
      state: SerializedStateRow,
      value: Any,
      changeEvent: Option[SerializedJournalRow]): Future[Option[Instant]] = {
    require(state.revision > 0)
    val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)
    val executor = executorProvider.executorFor(slice)

    def change =
      new UpdatedDurableState[Any](state.persistenceId, state.revision, value, NoOffset, EmptyDbTimestamp.toEpochMilli)

    val entityType = PersistenceId.extractEntityType(state.persistenceId)

    val result: Future[(Long, Option[Instant])] = {

      shouldInsert(state).flatMap {
        case true =>
          // insert
          def recoverDataIntegrityViolation[A](f: Future[A]): Future[A] =
            f.recoverWith { case _: R2dbcDataIntegrityViolationException =>
              Future.failed(
                new IllegalStateException(
                  s"Insert failed: durable state for persistence id [${state.persistenceId}] already exists"))
            }

          if (!changeHandlers.contains(entityType) && changeEvent.isEmpty) {
            val insertedRows =
              recoverDataIntegrityViolation(executor.updateOne(s"insert [${state.persistenceId}]") { connection =>
                insertStatement(entityType, state, value, slice, connection)
              })
            insertedRows.map(_ -> None)
          } else
            executor.withConnection(s"insert [${state.persistenceId}]") { connection =>
              for {
                insertedRows <- recoverDataIntegrityViolation(
                  R2dbcExecutor.updateOneInTx(insertStatement(entityType, state, value, slice, connection)))
                changeEventTimestamp <- writeChangeEventAndCallChangeHandler(
                  connection,
                  insertedRows,
                  entityType,
                  change,
                  changeEvent)
              } yield (insertedRows, changeEventTimestamp)
            }
        case false =>
          // update
          val previousRevision = state.revision - 1
          if (!changeHandlers.contains(entityType) && changeEvent.isEmpty) {
            val updatedRows = executor.updateOne(s"update [${state.persistenceId}]") { connection =>
              updateStatement(entityType, state, value, slice, connection, previousRevision)
            }
            updatedRows.map(_ -> None)
          } else
            executor.withConnection(s"update [${state.persistenceId}]") { connection =>
              for {
                updatedRows <- R2dbcExecutor.updateOneInTx(
                  updateStatement(entityType, state, value, slice, connection, previousRevision))
                changeEventTimestamp <- writeChangeEventAndCallChangeHandler(
                  connection,
                  updatedRows,
                  entityType,
                  change,
                  changeEvent)
              } yield (updatedRows, changeEventTimestamp)
            }
      }
    }

    result.map { case (updatedRows, changeEventTimestamp) =>
      if (updatedRows != 1)
        throw new IllegalStateException(
          s"Update failed: durable state for persistence id [${state.persistenceId}] could not be updated to revision [${state.revision}]")
      else {
        log.debug("Updated durable state for persistenceId [{}] to revision [{}]", state.persistenceId, state.revision)
        changeEventTimestamp
      }
    }
  }

  private def processChange(
      handler: ChangeHandler[Any],
      connection: Connection,
      change: DurableStateChange[Any]): Future[Done] = {
    val session = new R2dbcSession(connection)(ec, system)

    def excMessage(cause: Throwable): String = {
      val (changeType, revision) = change match {
        case upd: UpdatedDurableState[_] => "update" -> upd.revision
        case del: DeletedDurableState[_] => "delete" -> del.revision
      }
      s"Change handler $changeType failed for [${change.persistenceId}] revision [$revision], due to ${cause.getMessage}"
    }

    try handler.process(session, change).recoverWith { case cause =>
      Future.failed[Done](new ChangeHandlerException(excMessage(cause), cause))
    } catch {
      case NonFatal(cause) => throw new ChangeHandlerException(excMessage(cause), cause)
    }
  }

  override def deleteState(
      persistenceId: String,
      revision: Long,
      changeEvent: Option[SerializedJournalRow]): Future[Option[Instant]] = {
    if (revision == 0) {
      hardDeleteState(persistenceId)
        .map(_ => None)(ExecutionContext.parasitic)
    } else {
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      val executor = executorProvider.executorFor(slice)
      val result: Future[(Long, Option[Instant])] = {
        val entityType = PersistenceId.extractEntityType(persistenceId)
        def change =
          new DeletedDurableState[Any](persistenceId, revision, NoOffset, EmptyDbTimestamp.toEpochMilli)
        if (revision == 1) {
          val slice = persistenceExt.sliceForPersistenceId(persistenceId)

          def insertDeleteMarkerStatement(connection: Connection): Statement = {
            val stmt = connection
              .createStatement(
                insertStateSql(slice, entityType, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL
              .bind(0, slice)
              .bind(1, entityType)
              .bind(2, persistenceId)
              .bind(3, revision)
              .bind(4, 0)
              .bind(5, "")
              .bindPayloadOption(6, None)
              .bindTagsNull(7)

            bindTimestampNow(stmt, () => 8)
          }

          def recoverDataIntegrityViolation[A](f: Future[A]): Future[A] =
            f.recoverWith { case _: R2dbcDataIntegrityViolationException =>
              Future.failed(new IllegalStateException(
                s"Insert delete marker with revision 1 failed: durable state for persistence id [$persistenceId] already exists"))
            }

          executor.withConnection(s"insert delete marker [$persistenceId]") { connection =>
            for {
              updatedRows <- recoverDataIntegrityViolation(
                R2dbcExecutor.updateOneInTx(insertDeleteMarkerStatement(connection)))
              changeEventTimestamp <- writeChangeEventAndCallChangeHandler(
                connection,
                updatedRows,
                entityType,
                change,
                changeEvent)
            } yield (updatedRows, changeEventTimestamp)
          }

        } else {
          val previousRevision = revision - 1

          def updateStatement(connection: Connection): Statement = {
            val idx = Iterator.range(0, Int.MaxValue)

            val stmt = connection
              .createStatement(
                updateStateSql(slice, entityType, updateTags = false, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL
              .bind(idx.next(), revision)
              .bind(idx.next(), 0)
              .bind(idx.next(), "")
              .bindPayloadOption(idx.next(), None)

            bindTimestampNow(stmt, idx.next)

            if (settings.dbTimestampMonotonicIncreasing) {
              if (settings.durableStateAssertSingleWriter)
                stmt
                  .bind(idx.next(), persistenceId)
                  .bind(idx.next(), previousRevision)
              else
                stmt
                  .bind(idx.next(), persistenceId)
            } else {
              stmt
                .bind(idx.next(), persistenceId)
                .bind(idx.next(), previousRevision)
                .bind(idx.next(), persistenceId)

              if (settings.durableStateAssertSingleWriter)
                stmt.bind(idx.next(), previousRevision)
              else
                stmt
            }
          }

          executor.withConnection(s"delete [$persistenceId]") { connection =>
            for {
              updatedRows <- R2dbcExecutor.updateOneInTx(updateStatement(connection))
              changeEventTimestamp <- writeChangeEventAndCallChangeHandler(
                connection,
                updatedRows,
                entityType,
                change,
                changeEvent)
            } yield (updatedRows, changeEventTimestamp)
          }
        }
      }

      result.map { case (updatedRows, changeEventTimestamp) =>
        if (updatedRows != 1)
          throw new IllegalStateException(
            s"Delete failed: durable state for persistence id [$persistenceId] could not be updated to revision [$revision]")
        else {
          log.debug("Deleted durable state for persistenceId [{}] to revision [{}]", persistenceId, revision)
          changeEventTimestamp
        }
      }

    }
  }

  private def hardDeleteState(persistenceId: String): Future[Done] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)

    val changeHandler = changeHandlers.get(entityType)
    val changeHandlerHint = changeHandler.map(_ => " with change handler").getOrElse("")

    val result =
      executor.withConnection(s"hard delete [$persistenceId]$changeHandlerHint") { connection =>
        for {
          updatedRows <- R2dbcExecutor.updateOneInTx(
            connection
              .createStatement(hardDeleteStateSql(entityType, slice))
              .bind(0, persistenceId))
          _ <- {
            val change = new DeletedDurableState[Any](persistenceId, 0L, NoOffset, EmptyDbTimestamp.toEpochMilli)
            writeChangeEventAndCallChangeHandler(connection, updatedRows, entityType, change, changeEvent = None)
          }
        } yield updatedRows
      }

    if (log.isDebugEnabled())
      result.foreach(_ => log.debug("Hard deleted durable state for persistenceId [{}]", persistenceId))

    result.map(_ => Done)(ExecutionContext.parasitic)
  }

  override def currentDbTimestamp(slice: Int): Future[Instant] = {
    val executor = executorProvider.executorFor(slice)
    executor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.getTimestamp("db_timestamp"))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  /**
   * behindCurrentTime is not used for postgres but for sqlserver
   */
  protected def bindStateBySlicesRange(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration): Statement = {
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
    toTimestamp match {
      case Some(until) =>
        stmt.bindTimestamp(2, until)
        stmt.bind(3, settings.querySettings.bufferSize)
      case None =>
        stmt.bind(2, settings.querySettings.bufferSize)
    }
  }

  override def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      fromSeqNr: Option[Long],
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedStateRow, NotUsed] = {
    if (!settings.isSliceRangeWithinSameDataPartition(minSlice, maxSlice))
      throw new IllegalArgumentException(
        s"Slice range [$minSlice-$maxSlice] spans over more than one " +
        s"of the [${settings.numberOfDataPartitions}] data partitions.")

    val executor = executorProvider.executorFor(minSlice)
    val result = executor.select(s"select stateBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(
            stateBySlicesRangeSql(
              entityType,
              maxDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
        bindStateBySlicesRange(stmt, entityType, fromTimestamp, toTimestamp, behindCurrentTime)
      },
      row =>
        if (backtracking) {
          val serId = row.get[Integer]("state_ser_id", classOf[Integer])
          // would have been better with an explicit deleted column as in the journal table,
          // but not worth the schema change
          val isDeleted = serId == 0

          SerializedStateRow(
            persistenceId = row.get("persistence_id", classOf[String]),
            revision = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            // payload = null => lazy loaded for backtracking (ugly, but not worth changing UpdatedDurableState in Akka)
            // payload = None => DeletedDurableState (no lazy loading)
            payload = if (isDeleted) None else null,
            serId = 0,
            serManifest = "",
            tags = Set.empty // tags not fetched in queries (yet)
          )
        } else
          SerializedStateRow(
            persistenceId = row.get("persistence_id", classOf[String]),
            revision = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = getPayload(row),
            serId = row.get[Integer]("state_ser_id", classOf[Integer]),
            serManifest = row.get("state_ser_manifest", classOf[String]),
            tags = Set.empty // tags not fetched in queries (yet)
          ))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] durable states from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    if (settings.durableStateTableByEntityTypeWithSchema.isEmpty && settings.numberOfDataPartitions == 1)
      persistenceIds(afterId, limit, settings.durableStateTableWithSchema(slice = 0), 0)
    else {
      def readFromCustomTables(
          acc: immutable.IndexedSeq[String],
          remainingEntityTypes: List[String],
          dataPartitionSlice: Int): Future[immutable.IndexedSeq[String]] = {
        if (acc.size >= limit) {
          Future.successful(acc)
        } else if (remainingEntityTypes.isEmpty) {
          Future.successful(acc)
        } else {
          val table = settings.getDurableStateTableWithSchema(remainingEntityTypes.head, dataPartitionSlice)
          readPersistenceIds(afterId, limit, table, dataPartitionSlice).flatMap { ids =>
            readFromCustomTables(acc ++ ids, remainingEntityTypes.tail, dataPartitionSlice)
          }
        }
      }

      val entityTypes = settings.durableStateTableByEntityTypeWithSchema.keys.toList.sorted
      val ids = {
        val idsPerSliceRange =
          settings.dataPartitionSliceRanges.map { sliceRange =>
            for {
              fromDefaultTable <- readPersistenceIds(
                afterId,
                limit,
                settings.durableStateTableWithSchema(sliceRange.min),
                sliceRange.min)
              fromCustomTables <- readFromCustomTables(Vector.empty, entityTypes, sliceRange.min)
            } yield {
              (fromDefaultTable ++ fromCustomTables)
            }
          }
        Future.sequence(idsPerSliceRange).map(_.flatten.sorted)
      }

      Source.futureSource(ids.map(Source(_))).take(limit).mapMaterializedValue(_ => NotUsed)
    }
  }

  override def persistenceIds(
      afterId: Option[String],
      limit: Long,
      table: String,
      dataPartitionSlice: Int): Source[String, NotUsed] = {
    val result = readPersistenceIds(afterId, limit, table, dataPartitionSlice)

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def bindAllPersistenceIdsAfterSql(stmt: Statement, after: String, limit: Long): Statement = {
    stmt
      .bind(0, after)
      .bind(1, limit)
  }

  private def readPersistenceIds(
      afterId: Option[String],
      limit: Long,
      table: String,
      dataPartitionSlice: Int): Future[immutable.IndexedSeq[String]] = {
    val executor = executorProvider.executorFor(dataPartitionSlice)
    val result = executor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(allPersistenceIdsAfterSql(table))
            bindAllPersistenceIdsAfterSql(stmt, after, limit)
          case None =>
            connection
              .createStatement(allPersistenceIdsSql(table))
              .bind(0, limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))
    result
  }

  protected def bindPersistenceIdsForEntityTypeSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityType + likeStmtPostfix)
      .bind(1, limit)
  }

  override def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val actualLimit = if (limit > Int.MaxValue) Int.MaxValue else limit.toInt
    val results: immutable.IndexedSeq[Future[immutable.IndexedSeq[String]]] =
      // query each data partition
      settings.dataPartitionSliceRanges.map { sliceRange =>
        val table = settings.getDurableStateTableWithSchema(entityType, sliceRange.min)
        val executor = executorProvider.executorFor(sliceRange.min)
        executor.select(s"select persistenceIds by entity type")(
          connection =>
            afterId match {
              case Some(after) =>
                val stmt = connection.createStatement(persistenceIdsForEntityTypeAfterSql(table))
                bindPersistenceIdsForEntityTypeAfterSql(stmt, entityType, likeStmtPostfix, after, actualLimit)

              case None =>
                val stmt = connection.createStatement(persistenceIdsForEntityTypeSql(table))
                bindPersistenceIdsForEntityTypeSql(stmt, entityType, likeStmtPostfix, actualLimit)
            },
          row => row.get("persistence_id", classOf[String]))
      }

    // Theoretically it could blow up with too many rows (> Int.MaxValue) when fetching from more than
    // one data partition, but we have other places with a hard limit of a total number of persistenceIds less
    // than Int.MaxValue.
    val combined: Future[immutable.IndexedSeq[String]] =
      if (results.size == 1) results.head // no data partitions
      else Future.sequence(results).map(_.flatten.sorted.take(actualLimit))

    if (log.isDebugEnabled)
      combined.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(combined.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  /**
   * Counts for a bucket may become inaccurate when existing durable state entities are updated since the timestamp is
   * changed.
   */
  override def countBucketsMayChange: Boolean = true

  protected def bindSelectBucketSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement =
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
      .bindTimestamp(2, toTimestamp)
      .bind(3, limit)

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val now = InstantFactory.now() // not important to use database time
    val toTimestamp = {
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val executor = executorProvider.executorFor(minSlice)
    val result = executor.select(s"select bucket counts [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection.createStatement(selectBucketsSql(entityType, minSlice, maxSlice))
        bindSelectBucketSql(stmt, entityType, fromTimestamp, toTimestamp, limit)
      },
      row => {
        val bucketStartEpochSeconds = row.get("bucket", classOf[java.lang.Long]).toLong * 10
        val count = row.get[java.lang.Long]("count", classOf[java.lang.Long]).toLong
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    if (toTimestamp == now)
      result
    else
      result.map(appendEmptyBucketIfLastIsMissing(_, toTimestamp))
  }

  private def additionalBindings(
      entityType: String,
      state: SerializedStateRow,
      value: Any): immutable.IndexedSeq[EvaluatedAdditionalColumnBindings] = additionalColumns.get(entityType) match {
    case None => Vector.empty[EvaluatedAdditionalColumnBindings]
    case Some(columns) =>
      val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)
      val upsert = AdditionalColumn.Upsert(state.persistenceId, entityType, slice, state.revision, value)
      columns.map(c => EvaluatedAdditionalColumnBindings(c, c.bind(upsert)))
  }

  private def bindTags(state: SerializedStateRow, stmt: Statement, i: Int): Statement = {
    if (state.tags.isEmpty)
      stmt.bindTagsNull(i)
    else
      stmt.bindTags(i, state.tags)
  }

  private def bindAdditionalColumns(
      stmt: Statement,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      nextIndex: () => Int): Statement = {
    additionalBindings.foreach {
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindValue(v)) =>
        stmt.bind(nextIndex(), v)
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
        stmt.bindNull(nextIndex(), col.fieldClass)
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
    }
    stmt
  }

  private def updateStatement(
      entityType: String,
      state: SerializedStateRow,
      value: Any,
      slice: Int,
      connection: Connection,
      previousRevision: Long): Statement = {
    val idx = Iterator.range(0, Int.MaxValue)
    val bindings = additionalBindings(entityType, state, value)
    val stmt = connection
      .createStatement(updateStateSql(slice, entityType, updateTags = true, bindings))
      .bind(idx.next(), state.revision)
      .bind(idx.next(), state.serId)
      .bind(idx.next(), state.serManifest)
      .bindPayloadOption(idx.next(), state.payload)
    bindTags(state, stmt, idx.next())
    bindAdditionalColumns(stmt, bindings, idx.next)

    bindTimestampNow(stmt, idx.next)

    if (settings.dbTimestampMonotonicIncreasing) {
      if (settings.durableStateAssertSingleWriter)
        stmt
          .bind(idx.next(), state.persistenceId)
          .bind(idx.next(), previousRevision)
      else
        stmt
          .bind(idx.next(), state.persistenceId)
    } else {
      stmt
        .bind(idx.next(), state.persistenceId)
        .bind(idx.next(), previousRevision)
        .bind(idx.next(), state.persistenceId)

      if (settings.durableStateAssertSingleWriter)
        stmt.bind(idx.next(), previousRevision)
      else
        stmt
    }
  }

  private def insertStatement(
      entityType: String,
      state: SerializedStateRow,
      value: Any,
      slice: Int,
      connection: Connection): Statement = {
    val idx = Iterator.range(0, Int.MaxValue)
    val bindings = additionalBindings(entityType, state, value)
    val stmt = connection
      .createStatement(insertStateSql(slice, entityType, bindings))
      .bind(idx.next(), slice)
      .bind(idx.next(), entityType)
      .bind(idx.next(), state.persistenceId)
      .bind(idx.next(), state.revision)
      .bind(idx.next(), state.serId)
      .bind(idx.next(), state.serManifest)
      .bindPayloadOption(idx.next(), state.payload)
    bindTags(state, stmt, idx.next())
    bindAdditionalColumns(stmt, bindings, idx.next)

    bindTimestampNow(stmt, idx.next)
    stmt
  }

}
