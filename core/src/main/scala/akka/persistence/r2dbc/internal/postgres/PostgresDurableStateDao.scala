/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
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
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
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
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.SqlServerQueryAdapter
import akka.persistence.r2dbc.internal.codec.TagsCodec
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec
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
private[r2dbc] class PostgresDurableStateDao(
    settings: R2dbcSettings,
    connectionFactory: ConnectionFactory,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends DurableStateDao {
  import DurableStateDao._
  import PostgresDurableStateDao._
  protected def log: Logger = PostgresDurableStateDao.log

  private val persistenceExt = Persistence(system)
  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  private implicit val statePayloadCodec: PayloadCodec = settings.durableStatePayloadCodec
  implicit val timestampCodec: TimestampCodec = settings.timestampCodec
  private implicit val tagsCodec: TagsCodec = settings.tagsCodec
  implicit val queryAdapter: QueryAdapter = settings.queryAdapter

  // used for change events
  private lazy val journalDao: JournalDao = dialect.createJournalDao(settings, connectionFactory)

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

  protected def selectStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = ?"""
  }

  protected def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
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
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
    INSERT INTO $stateTable
    (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?$additionalParams, CURRENT_TIMESTAMP)"""
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
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings],
      currentTimestamp: String = "CURRENT_TIMESTAMP"): String = {

    val stateTable = settings.getDurableStateTableWithSchema(entityType)

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

  protected def additionalUpdateParameters(
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

  protected def hardDeleteStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"DELETE from $stateTable WHERE persistence_id = ?"
  }

  private val currentDbTimestampSql =
    sql"SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def allPersistenceIdsSql(table: String): String =
    sql"SELECT persistence_id from $table ORDER BY persistence_id LIMIT ?"

  protected def persistenceIdsForEntityTypeSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"

  protected def allPersistenceIdsAfterSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

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

  protected def persistenceIdsForEntityTypeAfterSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"

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
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

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
    r2dbcExecutor.selectOne(s"select [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectStateSql(entityType))
          .bind(0, persistenceId),
      row =>
        SerializedStateRow(
          persistenceId = persistenceId,
          revision = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
          dbTimestamp = row.getTimestamp(),
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
    if (serId == 0 && (rowPayload == null || util.Arrays.equals(statePayloadCodec.nonePayload, rowPayload)))
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

    def bindTags(stmt: Statement, i: Int): Statement = {
      if (state.tags.isEmpty)
        stmt.bindTagsNull(i)
      else
        stmt.bindTags(i, state.tags)
    }

    var i = 0

    def getAndIncIndex(): Int = {
      i += 1
      i - 1
    }

    def bindAdditionalColumns(
        stmt: Statement,
        additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindValue(v)) =>
          stmt.bind(getAndIncIndex(), v)
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
          stmt.bindNull(getAndIncIndex(), col.fieldClass)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      stmt
    }

    def change =
      new UpdatedDurableState[Any](state.persistenceId, state.revision, value, NoOffset, EmptyDbTimestamp.toEpochMilli)

    val entityType = PersistenceId.extractEntityType(state.persistenceId)

    val result: Future[(Long, Option[Instant])] = {
      val additionalBindings = additionalColumns.get(entityType) match {
        case None => Vector.empty[EvaluatedAdditionalColumnBindings]
        case Some(columns) =>
          val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)
          val upsert = AdditionalColumn.Upsert(state.persistenceId, entityType, slice, state.revision, value)
          columns.map(c => EvaluatedAdditionalColumnBindings(c, c.bind(upsert)))
      }

      if (state.revision == 1) {
        val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)
        def insertStatement(connection: Connection): Statement = {
          val stmt = connection
            .createStatement(insertStateSql(entityType, additionalBindings))
            .bind(getAndIncIndex(), slice)
            .bind(getAndIncIndex(), entityType)
            .bind(getAndIncIndex(), state.persistenceId)
            .bind(getAndIncIndex(), state.revision)
            .bind(getAndIncIndex(), state.serId)
            .bind(getAndIncIndex(), state.serManifest)
            .bindPayloadOption(getAndIncIndex(), state.payload)
          bindTags(stmt, getAndIncIndex())
          bindAdditionalColumns(stmt, additionalBindings)

          bindTimestampNow(stmt, getAndIncIndex)
          stmt
        }

        def recoverDataIntegrityViolation[A](f: Future[A]): Future[A] =
          f.recoverWith { case _: R2dbcDataIntegrityViolationException =>
            Future.failed(
              new IllegalStateException(
                s"Insert failed: durable state for persistence id [${state.persistenceId}] already exists"))
          }

        if (!changeHandlers.contains(entityType) && changeEvent.isEmpty) {
          val updatedRows = recoverDataIntegrityViolation(
            r2dbcExecutor.updateOne(s"insert [${state.persistenceId}]")(insertStatement))
          updatedRows.map(_ -> None)
        } else
          r2dbcExecutor.withConnection(s"insert [${state.persistenceId}]") { connection =>
            for {
              updatedRows <- recoverDataIntegrityViolation(R2dbcExecutor.updateOneInTx(insertStatement(connection)))
              changeEventTimestamp <- writeChangeEventAndCallChangeHandler(
                connection,
                updatedRows,
                entityType,
                change,
                changeEvent)
            } yield (updatedRows, changeEventTimestamp)
          }
      } else {
        val previousRevision = state.revision - 1

        def updateStatement(connection: Connection): Statement = {
          val stmt = connection
            .createStatement(updateStateSql(entityType, updateTags = true, additionalBindings))
            .bind(getAndIncIndex(), state.revision)
            .bind(getAndIncIndex(), state.serId)
            .bind(getAndIncIndex(), state.serManifest)
            .bindPayloadOption(getAndIncIndex(), state.payload)
          bindTags(stmt, getAndIncIndex())
          bindAdditionalColumns(stmt, additionalBindings)

          bindTimestampNow(stmt, getAndIncIndex)

          if (settings.dbTimestampMonotonicIncreasing) {
            if (settings.durableStateAssertSingleWriter)
              stmt
                .bind(getAndIncIndex(), state.persistenceId)
                .bind(getAndIncIndex(), previousRevision)
            else
              stmt
                .bind(getAndIncIndex(), state.persistenceId)
          } else {
            stmt
              .bind(getAndIncIndex(), state.persistenceId)
              .bind(getAndIncIndex(), previousRevision)
              .bind(getAndIncIndex(), state.persistenceId)

            if (settings.durableStateAssertSingleWriter)
              stmt.bind(getAndIncIndex(), previousRevision)
            else
              stmt
          }
        }

        if (!changeHandlers.contains(entityType) && changeEvent.isEmpty) {
          val updatedRows = r2dbcExecutor.updateOne(s"update [${state.persistenceId}]")(updateStatement)
          updatedRows.map(_ -> None)
        } else
          r2dbcExecutor.withConnection(s"update [${state.persistenceId}]") { connection =>
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
    val session = new R2dbcSession(connection)

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
        .map(_ => None)(ExecutionContexts.parasitic)
    } else {
      val result: Future[(Long, Option[Instant])] = {
        val entityType = PersistenceId.extractEntityType(persistenceId)
        def change =
          new DeletedDurableState[Any](persistenceId, revision, NoOffset, EmptyDbTimestamp.toEpochMilli)
        if (revision == 1) {
          val slice = persistenceExt.sliceForPersistenceId(persistenceId)

          def insertDeleteMarkerStatement(connection: Connection): Statement = {
            val stmt = connection
              .createStatement(
                insertStateSql(entityType, Vector.empty)
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

          r2dbcExecutor.withConnection(s"insert delete marker [$persistenceId]") { connection =>
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
            var i = 0
            def getAndIncIndex(): Int = {
              i += 1
              i - 1
            }

            val stmt = connection
              .createStatement(
                updateStateSql(entityType, updateTags = false, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL
              .bind(getAndIncIndex(), revision)
              .bind(getAndIncIndex(), 0)
              .bind(getAndIncIndex(), "")
              .bindPayloadOption(getAndIncIndex(), None)

            bindTimestampNow(stmt, getAndIncIndex)

            if (settings.dbTimestampMonotonicIncreasing) {
              if (settings.durableStateAssertSingleWriter)
                stmt
                  .bind(getAndIncIndex(), persistenceId)
                  .bind(getAndIncIndex(), previousRevision)
              else
                stmt
                  .bind(getAndIncIndex(), persistenceId)
            } else {
              stmt
                .bind(getAndIncIndex(), persistenceId)
                .bind(getAndIncIndex(), previousRevision)
                .bind(getAndIncIndex(), persistenceId)

              if (settings.durableStateAssertSingleWriter)
                stmt.bind(getAndIncIndex(), previousRevision)
              else
                stmt
            }
          }

          r2dbcExecutor.withConnection(s"delete [$persistenceId]") { connection =>
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

    val changeHandler = changeHandlers.get(entityType)
    val changeHandlerHint = changeHandler.map(_ => " with change handler").getOrElse("")

    val result =
      r2dbcExecutor.withConnection(s"hard delete [$persistenceId]$changeHandlerHint") { connection =>
        for {
          updatedRows <- R2dbcExecutor.updateOneInTx(
            connection
              .createStatement(hardDeleteStateSql(entityType))
              .bind(0, persistenceId))
          _ <- {
            val change = new DeletedDurableState[Any](persistenceId, 0L, NoOffset, EmptyDbTimestamp.toEpochMilli)
            writeChangeEventAndCallChangeHandler(connection, updatedRows, entityType, change, changeEvent = None)
          }
        } yield updatedRows
      }

    if (log.isDebugEnabled())
      result.foreach(_ => log.debug("Hard deleted durable state for persistenceId [{}]", persistenceId))

    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

  override def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.getTimestamp())
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
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedStateRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select stateBySlices [$minSlice - $maxSlice]")(
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
            dbTimestamp = row.getTimestamp(),
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
            dbTimestamp = row.getTimestamp(),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = getPayload(row),
            serId = row.get[Integer]("state_ser_id", classOf[Integer]),
            serManifest = row.get("state_ser_manifest", classOf[String]),
            tags = Set.empty // tags not fetched in queries (yet)
          ))

    if (log.isDebugEnabled)
      result.foreach(rows =>
        log.debugN("Read [{}] durable states from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    if (settings.durableStateTableByEntityTypeWithSchema.isEmpty)
      persistenceIds(afterId, limit, settings.durableStateTableWithSchema)
    else {
      def readFromCustomTables(
          acc: immutable.IndexedSeq[String],
          remainingTables: Vector[String]): Future[immutable.IndexedSeq[String]] = {
        if (acc.size >= limit) {
          Future.successful(acc)
        } else if (remainingTables.isEmpty) {
          Future.successful(acc)
        } else {
          readPersistenceIds(afterId, limit, remainingTables.head).flatMap { ids =>
            readFromCustomTables(acc ++ ids, remainingTables.tail)
          }
        }
      }

      val customTables = settings.durableStateTableByEntityTypeWithSchema.toVector.sortBy(_._1).map(_._2)
      val ids = for {
        fromDefaultTable <- readPersistenceIds(afterId, limit, settings.durableStateTableWithSchema)
        fromCustomTables <- readFromCustomTables(Vector.empty, customTables)
      } yield {
        (fromDefaultTable ++ fromCustomTables).sorted
      }

      Source.futureSource(ids.map(Source(_))).take(limit).mapMaterializedValue(_ => NotUsed)
    }
  }

  override def persistenceIds(afterId: Option[String], limit: Long, table: String): Source[String, NotUsed] = {
    val result = readPersistenceIds(afterId, limit, table)

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
      table: String): Future[immutable.IndexedSeq[String]] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
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
    val table = settings.getDurableStateTableWithSchema(entityType)
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(persistenceIdsForEntityTypeAfterSql(table))
            bindPersistenceIdsForEntityTypeAfterSql(stmt, entityType, likeStmtPostfix, after, limit)
          case None =>
            val stmt = connection.createStatement(persistenceIdsForEntityTypeSql(table))
            bindPersistenceIdsForEntityTypeSql(stmt, entityType, likeStmtPostfix, limit)

        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
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

    val toTimestamp = {
      val now = InstantFactory.now() // not important to use database time
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$minSlice - $maxSlice]")(
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
      result.foreach(rows => log.debugN("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result

  }
}
