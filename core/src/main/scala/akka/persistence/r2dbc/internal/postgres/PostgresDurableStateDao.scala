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
import akka.persistence.r2dbc.internal.{
  AdditionalColumnFactory,
  ChangeHandlerFactory,
  Dialect,
  DurableStateDao,
  InstantFactory,
  JournalDao,
  PayloadCodec,
  R2dbcExecutor,
  TimestampCodec
}
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.TimestampCodec.{
  RichRow => TimestampRichRow,
  RichStatement => TimestampRichStatement
}
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.sql.{ BaseDurableStateSql, PostgresDurableStateSql }
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

  // move this to a dialect independent place?
  final case class EvaluatedAdditionalColumnBindings(
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

  protected implicit val statePayloadCodec: PayloadCodec = settings.durableStatePayloadCodec
  protected implicit val timestampCodec: TimestampCodec = settings.timestampCodec

  val durableStateSql: BaseDurableStateSql = new PostgresDurableStateSql(settings)

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

  private val currentDbTimestampSql =
    sql"SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  override def readState(persistenceId: String): Future[Option[SerializedStateRow]] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    r2dbcExecutor.selectOne(s"select [$persistenceId]")(
      { connection =>
        val stmt = connection.createStatement(durableStateSql.selectStateSql(entityType))
        durableStateSql.bindForSelectStateSql(stmt, persistenceId)

      },
      (row: Row) =>
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

  private def writeChangeEventAndCallChangeHander(
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

  override def upsertState(
      state: SerializedStateRow,
      value: Any,
      changeEvent: Option[SerializedJournalRow]): Future[Option[Instant]] = {
    require(state.revision > 0)

    def bindTags(stmt: Statement, i: Int): Statement = {
      if (state.tags.isEmpty)
        stmt.bindNull(i, classOf[Array[String]])
      else
        stmt.bind(i, state.tags.toArray)
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
            .createStatement(durableStateSql.insertStateSql(entityType, additionalBindings))
          durableStateSql.bindInsertStateForUpsertState(
            stmt,
            getAndIncIndex,
            slice,
            entityType,
            state,
            additionalBindings)
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
              changeEventTimestamp <- writeChangeEventAndCallChangeHander(
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
            .createStatement(durableStateSql.updateStateSql(entityType, updateTags = true, additionalBindings))
          durableStateSql.binUpdateStateSqlForUpsertState(
            stmt,
            getAndIncIndex,
            state,
            additionalBindings,
            previousRevision)
        }

        if (!changeHandlers.contains(entityType) && changeEvent.isEmpty) {
          val updatedRows = r2dbcExecutor.updateOne(s"update [${state.persistenceId}]")(updateStatement)
          updatedRows.map(_ -> None)
        } else
          r2dbcExecutor.withConnection(s"update [${state.persistenceId}]") { connection =>
            for {
              updatedRows <- R2dbcExecutor.updateOneInTx(updateStatement(connection))
              changeEventTimestamp <- writeChangeEventAndCallChangeHander(
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
                durableStateSql.insertStateSql(entityType, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL

            durableStateSql.bindDeleteStateForInsertState(stmt, slice, entityType, persistenceId, revision)
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
              changeEventTimestamp <- writeChangeEventAndCallChangeHander(
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
            val stmt = connection
              .createStatement(
                durableStateSql.updateStateSql(entityType, updateTags = false, Vector.empty)
              ) // FIXME should the additional columns be cleared (null)? Then they must allow NULL
            durableStateSql.bindUpdateStateSqlForDeleteState(stmt, revision, persistenceId, previousRevision)
          }

          r2dbcExecutor.withConnection(s"delete [$persistenceId]") { connection =>
            for {
              updatedRows <- R2dbcExecutor.updateOneInTx(updateStatement(connection))
              changeEventTimestamp <- writeChangeEventAndCallChangeHander(
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
          updatedRows <- R2dbcExecutor.updateOneInTx {
            val stmt = connection.createStatement(durableStateSql.hardDeleteStateSql(entityType))
            durableStateSql.bindForHardDeleteState(stmt, persistenceId)
          }
          _ <- {
            val change = new DeletedDurableState[Any](persistenceId, 0L, NoOffset, EmptyDbTimestamp.toEpochMilli)
            writeChangeEventAndCallChangeHander(connection, updatedRows, entityType, change, changeEvent = None)
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
        row => row.get("db_timestamp", classOf[Instant]))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
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
            durableStateSql.stateBySlicesRangeSql(
              entityType,
              maxDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
        durableStateSql.bindForStateBySlicesRangeSql(stmt, entityType, fromTimestamp, toTimestamp, behindCurrentTime)
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

  def readPersistenceIds(afterId: Option[String], limit: Long, table: String): Future[immutable.IndexedSeq[String]] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(durableStateSql.allPersistenceIdsAfterSql(table))
            durableStateSql.bindForAllPersistenceIdsAfter(stmt, after, limit)

          case None =>
            val stmt = connection.createStatement(durableStateSql.allPersistenceIdsSql(table))
            durableStateSql.bindForAllPersistenceIdsSql(stmt, limit)

        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))
    result
  }

  override def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val table = settings.getDurableStateTableWithSchema(entityType)
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(durableStateSql.persistenceIdsForEntityTypeAfterSql(table))
            durableStateSql.bindPersistenceIdsForEntityTypeAfter(stmt, entityType + likeStmtPostfix, after, limit)

          case None =>
            val stmt = connection.createStatement(durableStateSql.persistenceIdsForEntityTypeSql(table))
            durableStateSql.bindPersistenceIdsForEntityType(stmt, entityType + likeStmtPostfix, limit)

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

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val now = timestampCodec.now() // not important to use database time
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
        val stmt = connection.createStatement(durableStateSql.selectBucketsSql(entityType, minSlice, maxSlice))
        durableStateSql.bindSelectBucketsForCoundBuckets(stmt, entityType, fromTimestamp, toTimestamp, limit)
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
