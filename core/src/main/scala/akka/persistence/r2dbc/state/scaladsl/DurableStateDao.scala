/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.Dialect
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object DurableStateDao {
  val log: Logger = LoggerFactory.getLogger(classOf[DurableStateDao])
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedStateRow(
      persistenceId: String,
      revision: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Array[Byte],
      serId: Int,
      serManifest: String,
      tags: Set[String])
      extends BySliceQuery.SerializedRow {
    override def seqNr: Long = revision
  }
}

/**
 * INTERNAL API
 *
 * Class for encapsulating db interaction.
 */
@InternalApi
private[r2dbc] class DurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends BySliceQuery.Dao[DurableStateDao.SerializedStateRow] {
  import DurableStateDao._

  private val persistenceExt = Persistence(system)
  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  private val stateTable = settings.durableStateTableWithSchema

  private val selectStateSql: String = sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = ?"""

  private def selectBucketsSql(minSlice: Int, maxSlice: Int): String = {
    sql"""
     SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
     FROM $stateTable
     WHERE entity_type = ?
     AND ${sliceCondition(minSlice, maxSlice)}
     AND db_timestamp >= ? AND db_timestamp <= ?
     GROUP BY bucket ORDER BY bucket LIMIT ?
     """
  }

  private def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    settings.dialect match {
      case Dialect.Yugabyte => s"slice BETWEEN $minSlice AND $maxSlice"
      case Dialect.Postgres => s"slice in (${(minSlice to maxSlice).mkString(",")})"
    }
  }

  private val insertStateSql: String = sql"""
    INSERT INTO $stateTable
    (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags, db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, transaction_timestamp())"""

  private val updateStateSql: String = {
    val timestamp =
      if (settings.dbTimestampMonotonicIncreasing)
        "transaction_timestamp()"
      else
        "GREATEST(transaction_timestamp(), " +
        s"(SELECT db_timestamp + '1 microsecond'::interval FROM $stateTable WHERE persistence_id = ? AND revision = ?))"

    val revisionCondition =
      if (settings.durableStateAssertSingleWriter) " AND revision = ?"
      else ""

    sql"""
      UPDATE $stateTable
      SET revision = ?, state_ser_id = ?, state_ser_manifest = ?, state_payload = ?, tags = ?, db_timestamp = $timestamp
      WHERE persistence_id = ?
      $revisionCondition"""
  }

  private val deleteStateSql: String =
    sql"DELETE from $stateTable WHERE persistence_id = ?"

  private val currentDbTimestampSql =
    sql"SELECT transaction_timestamp() AS db_timestamp"

  private val allPersistenceIdsSql =
    sql"SELECT persistence_id from $stateTable ORDER BY persistence_id LIMIT ?"

  private val allPersistenceIdsAfterSql =
    sql"SELECT persistence_id from $stateTable WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

  private def stateBySlicesRangeSql(
      maxDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def maxDbTimestampParamCondition =
      if (maxDbTimestampParam) s"AND db_timestamp < ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < transaction_timestamp() - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    val selectColumns =
      if (backtracking)
        "SELECT persistence_id, revision, db_timestamp, statement_timestamp() AS read_db_timestamp "
      else
        "SELECT persistence_id, revision, db_timestamp, statement_timestamp() AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
      $selectColumns
      FROM $stateTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      ORDER BY db_timestamp, revision
      LIMIT ?"""
  }

  def readState(persistenceId: String): Future[Option[SerializedStateRow]] = {
    r2dbcExecutor.selectOne(s"select [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectStateSql)
          .bind(0, persistenceId),
      row =>
        SerializedStateRow(
          persistenceId = persistenceId,
          revision = row.get("revision", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = Instant.EPOCH, // not needed here
          payload = row.get("state_payload", classOf[Array[Byte]]),
          serId = row.get("state_ser_id", classOf[Integer]),
          serManifest = row.get("state_ser_manifest", classOf[String]),
          tags = Set.empty // tags not fetched in queries (yet)
        ))
  }

  def writeState(state: SerializedStateRow): Future[Done] = {
    require(state.revision > 0)

    val entityType = PersistenceId.extractEntityType(state.persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)

    def bindTags(stmt: Statement, i: Int): Statement = {
      if (state.tags.isEmpty)
        stmt.bindNull(i, classOf[Array[String]])
      else
        stmt.bind(i, state.tags.toArray)
    }

    val result = {
      if (state.revision == 1) {
        r2dbcExecutor
          .updateOne(s"insert [${state.persistenceId}]") { connection =>
            val stmt = connection
              .createStatement(insertStateSql)
              .bind(0, slice)
              .bind(1, entityType)
              .bind(2, state.persistenceId)
              .bind(3, state.revision)
              .bind(4, state.serId)
              .bind(5, state.serManifest)
              .bind(6, state.payload)
            bindTags(stmt, 7)
          }
          .recoverWith { case _: R2dbcDataIntegrityViolationException =>
            Future.failed(
              new IllegalStateException(
                s"Insert failed: durable state for persistence id [${state.persistenceId}] already exists"))
          }
      } else {
        val previousRevision = state.revision - 1

        r2dbcExecutor.updateOne(s"update [${state.persistenceId}]") { connection =>
          val stmt = connection
            .createStatement(updateStateSql)
            .bind(0, state.revision)
            .bind(1, state.serId)
            .bind(2, state.serManifest)
            .bind(3, state.payload)
          bindTags(stmt, 4)

          if (settings.dbTimestampMonotonicIncreasing) {
            if (settings.durableStateAssertSingleWriter)
              stmt
                .bind(5, state.persistenceId)
                .bind(6, previousRevision)
            else
              stmt
                .bind(5, state.persistenceId)
          } else {
            stmt
              .bind(5, state.persistenceId)
              .bind(6, previousRevision)
              .bind(7, state.persistenceId)

            if (settings.durableStateAssertSingleWriter)
              stmt.bind(8, previousRevision)
            else
              stmt
          }
        }
      }
    }

    result.map { updatedRows =>
      if (updatedRows != 1)
        throw new IllegalStateException(
          s"Update failed: durable state for persistence id [${state.persistenceId}] could not be updated to revision [${state.revision}]")
      else {
        log.debug("Updated durable state for persistenceId [{}] to revision [{}]", state.persistenceId, state.revision)
        Done
      }
    }
  }

  def deleteState(persistenceId: String): Future[Done] = {
    val result =
      r2dbcExecutor.updateOne(s"delete [$persistenceId]") { connection =>
        connection
          .createStatement(deleteStateSql)
          .bind(0, persistenceId)
      }

    if (log.isDebugEnabled())
      result.foreach(_ => log.debug("Deleted durable state for persistenceId [{}]", persistenceId))

    result.map(_ => Done)(ExecutionContext.parasitic)
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
            stateBySlicesRangeSql(
              maxDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
          .bind(0, entityType)
          .bind(1, fromTimestamp)
        toTimestamp match {
          case Some(until) =>
            stmt.bind(2, until)
            stmt.bind(3, settings.querySettings.bufferSize)
          case None =>
            stmt.bind(2, settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        if (backtracking)
          SerializedStateRow(
            persistenceId = row.get("persistence_id", classOf[String]),
            revision = row.get("revision", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = null, // lazy loaded for backtracking
            serId = 0,
            serManifest = "",
            tags = Set.empty // tags not fetched in queries (yet)
          )
        else
          SerializedStateRow(
            persistenceId = row.get("persistence_id", classOf[String]),
            revision = row.get("revision", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = row.get("state_payload", classOf[Array[Byte]]),
            serId = row.get("state_ser_id", classOf[Integer]),
            serManifest = row.get("state_ser_manifest", classOf[String]),
            tags = Set.empty // tags not fetched in queries (yet)
          ))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] durable states from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(allPersistenceIdsAfterSql)
              .bind(0, after)
              .bind(1, limit)
          case None =>
            connection
              .createStatement(allPersistenceIdsSql)
              .bind(0, limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  /**
   * Counts for a bucket may become inaccurate when existing durable state entities are updated since the timestamp is
   * changed.
   */
  def countBucketsMayChange: Boolean = true

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val now = Instant.now() // not important to use database time
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$minSlice - $maxSlice]")(
      connection =>
        connection
          .createStatement(selectBucketsSql(minSlice, maxSlice))
          .bind(0, entityType)
          .bind(1, fromTimestamp)
          .bind(2, toTimestamp)
          .bind(3, limit),
      row => {
        val bucketStartEpochSeconds = row.get("bucket", classOf[java.lang.Long]).toLong * 10
        val count = row.get("count", classOf[java.lang.Long]).toLong
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result

  }
}
