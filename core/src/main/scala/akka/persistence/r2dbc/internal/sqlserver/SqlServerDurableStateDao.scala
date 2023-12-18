/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

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
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.r2dbc.state.ChangeHandlerException
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.state.scaladsl.ChangeHandler
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang
import java.time.Instant
import java.time.LocalDateTime
import java.util
import java.util.TimeZone
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerDurableStateDao {

  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerDurableStateDao])

  private final case class EvaluatedAdditionalColumnBindings(
      additionalColumn: AdditionalColumn[_, _],
      binding: AdditionalColumn.Binding[_])

  val FutureDone: Future[Done] = Future.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class SqlServerDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends DurableStateDao {
  import DurableStateDao._
  import SqlServerDurableStateDao._
  protected def log: Logger = SqlServerDurableStateDao.log

  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)
  import helper._

  private val persistenceExt = Persistence(system)
  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  private implicit val statePayloadCodec: PayloadCodec = settings.durableStatePayloadCodec

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

  private def selectStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = @persistenceId"""
  }

  private def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    // group by column alias (bucket) needs a sub query
    val subQuery =
      s"""
         | select TOP(@limit) CAST(DATEDIFF(s,'1970-01-01 00:00:00',db_timestamp) AS BIGINT) / 10 AS bucket
         | FROM $stateTable
         | WHERE entity_type = @entityType
         | AND ${sliceCondition(minSlice, maxSlice)}
         | AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
         |""".stripMargin
    sql"""
     SELECT bucket,  count(*) as count from ($subQuery) as sub
     GROUP BY bucket ORDER BY bucket
     """
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  private def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
    INSERT INTO $stateTable
    (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
    VALUES (@slice, @entityType, @persistenceId, @revision, @stateSerId, @stateSerManifest, @statePayload, @tags$additionalParams, @now)"""
  }

  private def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, bindBalue) if bindBalue != AdditionalColumn.Skip =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

  private def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, bindValue) if bindValue != AdditionalColumn.Skip =>
          strB.append(s", @${col.columnName}")
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

  private def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    val revisionCondition =
      if (settings.durableStateAssertSingleWriter) " AND revision = @previousRevision"
      else ""

    val tags = if (updateTags) ", tags = @tags" else ""

    val additionalParams = additionalUpdateParameters(additionalBindings)
    sql"""
      UPDATE $stateTable
      SET revision = @revision, state_ser_id = @stateSerId, state_ser_manifest = @stateSerManifest, state_payload = @statePayload $tags $additionalParams, db_timestamp = @now
      WHERE persistence_id = @persistenceId
      $revisionCondition"""
  }

  private def additionalUpdateParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, binValue) if binValue != AdditionalColumn.Skip =>
          strB.append(", ").append(col.columnName).append(s" = @${col.columnName}")
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

  private def hardDeleteStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"DELETE from $stateTable WHERE persistence_id = @persistenceId"
  }

  private def allPersistenceIdsSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table ORDER BY persistence_id"

  private def persistenceIdsForEntityTypeSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id LIKE @persistenceIdLike ORDER BY persistence_id"

  private def allPersistenceIdsAfterSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id > @persistenceId ORDER BY persistence_id"

  private def persistenceIdsForEntityTypeAfterSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id LIKE @persistenceIdLike AND persistence_id > @persistenceId ORDER BY persistence_id"

  protected def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < DATEADD(ms, -${behindCurrentTime.toMillis}, @now)"
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
      if (maxDbTimestampParam) s"AND db_timestamp < @until" else ""

    val behindCurrentTimeIntervalCondition = behindCurrentTimeIntervalConditionFor(behindCurrentTime)

    val selectColumns =
      if (backtracking)
        "SELECT TOP(@limit) persistence_id, revision, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, state_ser_id "
      else
        "SELECT TOP(@limit) persistence_id, revision, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
      $selectColumns
      FROM $stateTable
      WHERE entity_type = @entityType
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= @fromTimestamp $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      ORDER BY db_timestamp, revision"""
  }

  def readState(persistenceId: String): Future[Option[SerializedStateRow]] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    r2dbcExecutor.selectOne(s"select [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectStateSql(entityType))
          .bind("@persistenceId", persistenceId),
      row =>
        SerializedStateRow(
          persistenceId = persistenceId,
          revision = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
          dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
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

  def upsertState(state: SerializedStateRow, value: Any): Future[Done] = {
    require(state.revision > 0)

    def bindTags(stmt: Statement, name: String): Statement = {
      if (state.tags.isEmpty)
        stmt.bindNull(name, classOf[String])
      else
        stmt.bind(name, tagsToDb(state.tags))
    }

    def bindAdditionalColumns(
        stmt: Statement,
        additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindValue(v)) =>
          stmt.bind(s"@${col.columnName}", v)
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
          stmt.bindNull(s"@${col.columnName}", col.fieldClass)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      stmt
    }

    def change =
      new UpdatedDurableState[Any](state.persistenceId, state.revision, value, NoOffset, EmptyDbTimestamp.toEpochMilli)

    val entityType = PersistenceId.extractEntityType(state.persistenceId)

    val result = {
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
            .bind("@slice", slice)
            .bind("@entityType", entityType)
            .bind("@persistenceId", state.persistenceId)
            .bind("@revision", state.revision)
            .bind("@stateSerId", state.serId)
            .bind("@stateSerManifest", state.serManifest)
            .bindPayloadOption("@statePayload", state.payload)
            .bind("@now", nowLocalDateTime())
          bindTags(stmt, "@tags")
          bindAdditionalColumns(stmt, additionalBindings)
        }

        def recoverDataIntegrityViolation[A](f: Future[A]): Future[A] =
          f.recoverWith { case _: R2dbcDataIntegrityViolationException =>
            Future.failed(
              new IllegalStateException(
                s"Insert failed: durable state for persistence id [${state.persistenceId}] already exists"))
          }

        changeHandlers.get(entityType) match {
          case None =>
            recoverDataIntegrityViolation(r2dbcExecutor.updateOne(s"insert [${state.persistenceId}]")(insertStatement))
          case Some(handler) =>
            r2dbcExecutor.withConnection(s"insert [${state.persistenceId}] with change handler") { connection =>
              for {
                updatedRows <- recoverDataIntegrityViolation(R2dbcExecutor.updateOneInTx(insertStatement(connection)))
                _ <- processChange(handler, connection, change)
              } yield updatedRows
            }
        }
      } else {
        val previousRevision = state.revision - 1

        def updateStatement(connection: Connection): Statement = {

          val query = updateStateSql(entityType, updateTags = true, additionalBindings)
          val stmt = connection
            .createStatement(query)
            .bind("@revision", state.revision)
            .bind("@stateSerId", state.serId)
            .bind("@stateSerManifest", state.serManifest)
            .bindPayloadOption("@statePayload", state.payload)
            .bind("@now", nowLocalDateTime())
            .bind("@persistenceId", state.persistenceId)
          bindTags(stmt, "@tags")
          bindAdditionalColumns(stmt, additionalBindings)

          if (settings.durableStateAssertSingleWriter) {
            stmt.bind("@previousRevision", previousRevision)
          }

          stmt
        }

        changeHandlers.get(entityType) match {
          case None =>
            r2dbcExecutor.updateOne(s"update [${state.persistenceId}]")(updateStatement)
          case Some(handler) =>
            r2dbcExecutor.withConnection(s"update [${state.persistenceId}] with change handler") { connection =>
              for {
                updatedRows <- R2dbcExecutor.updateOneInTx(updateStatement(connection))
                _ <- if (updatedRows == 1) processChange(handler, connection, change) else FutureDone
              } yield updatedRows
            }
        }
      }
    }

    result
      .map { updatedRows =>
        if (updatedRows != 1)
          throw new IllegalStateException(
            s"Update failed: durable state for persistence id [${state.persistenceId}] could not be updated to revision [${state.revision}]")
        else {
          log.debug(
            "Updated durable state for persistenceId [{}] to revision [{}]",
            state.persistenceId,
            state.revision)
          Done
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

  def deleteState(persistenceId: String, revision: Long): Future[Done] = {
    if (revision == 0) {
      hardDeleteState(persistenceId)
    } else {
      val result = {
        val entityType = PersistenceId.extractEntityType(persistenceId)
        def change =
          new DeletedDurableState[Any](persistenceId, revision, NoOffset, EmptyDbTimestamp.toEpochMilli)
        if (revision == 1) {
          val slice = persistenceExt.sliceForPersistenceId(persistenceId)

          def insertDeleteMarkerStatement(connection: Connection): Statement = {
            connection
              .createStatement(insertStateSql(entityType, Vector.empty))
              .bind("@slice", slice)
              .bind("@entityType", entityType)
              .bind("@persistenceId", persistenceId)
              .bind("@revision", revision)
              .bind("@stateSerId", 0)
              .bind("@stateSerManifest", "")
              .bindPayloadOption("@statePayload", None)
              .bindNull("@tags", classOf[String])
              .bind("@now", nowLocalDateTime())
          }

          def recoverDataIntegrityViolation[A](f: Future[A]): Future[A] =
            f.recoverWith { case _: R2dbcDataIntegrityViolationException =>
              Future.failed(new IllegalStateException(
                s"Insert delete marker with revision 1 failed: durable state for persistence id [$persistenceId] already exists"))
            }

          val changeHandler = changeHandlers.get(entityType)
          val changeHandlerHint = changeHandler.map(_ => " with change handler").getOrElse("")

          r2dbcExecutor.withConnection(s"insert delete marker [$persistenceId]$changeHandlerHint") { connection =>
            for {
              updatedRows <- recoverDataIntegrityViolation(
                R2dbcExecutor.updateOneInTx(insertDeleteMarkerStatement(connection)))
              _ <- changeHandler match {
                case None          => FutureDone
                case Some(handler) => processChange(handler, connection, change)
              }
            } yield updatedRows
          }

        } else {
          val previousRevision = revision - 1

          def updateStatement(connection: Connection): Statement = {
            connection
              .createStatement(
                updateStateSql(entityType, updateTags = false, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL
              .bind("@revision", revision)
              .bind("@stateSerId", 0)
              .bind("@stateSerManifest", "")
              .bindPayloadOption("@statePayload", None)
              .bind("@now", nowLocalDateTime())
              .bind("@persistenceId", persistenceId)
              .bind("@previousRevision", previousRevision)
          }

          val changeHandler = changeHandlers.get(entityType)
          val changeHandlerHint = changeHandler.map(_ => " with change handler").getOrElse("")

          r2dbcExecutor.withConnection(s"delete [$persistenceId]$changeHandlerHint") { connection =>
            for {
              updatedRows <- R2dbcExecutor.updateOneInTx(updateStatement(connection))
              _ <- changeHandler match {
                case None          => FutureDone
                case Some(handler) => processChange(handler, connection, change)
              }
            } yield updatedRows
          }
        }
      }

      result.map { updatedRows =>
        if (updatedRows != 1)
          throw new IllegalStateException(
            s"Delete failed: durable state for persistence id [$persistenceId] could not be updated to revision [$revision]")
        else {
          log.debug("Deleted durable state for persistenceId [{}] to revision [{}]", persistenceId, revision)
          Done
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
              .bind("@persistenceId", persistenceId))
          _ <- changeHandler match {
            case None => FutureDone
            case Some(handler) =>
              val change = new DeletedDurableState[Any](persistenceId, 0L, NoOffset, EmptyDbTimestamp.toEpochMilli)
              processChange(handler, connection, change)
          }
        } yield updatedRows
      }

    if (log.isDebugEnabled())
      result.foreach(_ => log.debug("Hard deleted durable state for persistenceId [{}]", persistenceId))

    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

  override def currentDbTimestamp(): Future[Instant] = Future.successful(nowInstant())

  override def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedStateRow, NotUsed] = {
    val result = r2dbcExecutor.select(
      s"select stateBySlices [${settings.querySettings.bufferSize}, $entityType, $fromTimestamp, $toTimestamp,  $minSlice - $maxSlice]")(
      connection => {
        val query = stateBySlicesRangeSql(
          entityType,
          maxDbTimestampParam = toTimestamp.isDefined,
          behindCurrentTime,
          backtracking,
          minSlice,
          maxSlice)
        val stmt = connection
          .createStatement(query)
          .bind("@entityType", entityType)
          .bind("@fromTimestamp", toDbTimestamp(fromTimestamp))

        stmt.bind("@limit", settings.querySettings.bufferSize)

        if (behindCurrentTime > Duration.Zero) {
          stmt.bind("@now", nowLocalDateTime())
        }

        toTimestamp.foreach(until => stmt.bind("@until", toDbTimestamp(until)))

        stmt
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
            dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
            readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
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
            dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
            readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
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

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
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

  def persistenceIds(afterId: Option[String], limit: Long, table: String): Source[String, NotUsed] = {
    val result = readPersistenceIds(afterId, limit, table)

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  private def readPersistenceIds(
      afterId: Option[String],
      limit: Long,
      table: String): Future[immutable.IndexedSeq[String]] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(allPersistenceIdsAfterSql(table))
              .bind("@persistenceId", after)
              .bind("@limit", limit)
          case None =>
            connection
              .createStatement(allPersistenceIdsSql(table))
              .bind("@limit", limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))
    result
  }

  def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val table = settings.getDurableStateTableWithSchema(entityType)
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(afterPersistenceId) =>
            connection
              .createStatement(persistenceIdsForEntityTypeAfterSql(table))
              .bind("@persistenceIdLike", entityType + likeStmtPostfix)
              .bind("@persistenceId", afterPersistenceId)
              .bind("@limit", limit)
          case None =>
            connection
              .createStatement(persistenceIdsForEntityTypeSql(table))
              .bind("@persistenceIdLike", entityType + likeStmtPostfix)
              .bind("@limit", limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  /**
   * Is this correct?
   */
  override def countBucketsMayChange: Boolean = true

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val nowTimestamp = nowInstant()
      if (fromTimestamp == Instant.EPOCH)
        nowTimestamp
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(nowTimestamp)) nowTimestamp else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$entityType $minSlice - $maxSlice]")(
      connection =>
        connection
          .createStatement(selectBucketsSql(entityType, minSlice, maxSlice))
          .bind("@entityType", entityType)
          .bind("@fromTimestamp", toDbTimestamp(fromTimestamp))
          .bind("@toTimestamp", toDbTimestamp(toTimestamp))
          .bind("@limit", limit),
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
