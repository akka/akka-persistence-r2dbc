/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object QueryDao {
  val log: Logger = LoggerFactory.getLogger(classOf[QueryDao])
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class QueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends BySliceQuery.Dao[SerializedJournalRow] {
  import QueryDao.log

  private val journalTable = settings.journalTableWithSchema

  private val currentDbTimestampSql =
    "SELECT transaction_timestamp() AS db_timestamp"

  private def eventsBySlicesRangeSql(maxDbTimestampParam: Boolean, behindCurrentTime: FiniteDuration): String = {
    var p = 0
    def nextParam(): String = {
      p += 1
      "$" + p
    }
    def maxDbTimestampParamCondition =
      if (maxDbTimestampParam) s"AND db_timestamp < ${nextParam()}" else ""
    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < transaction_timestamp() - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    s"""SELECT slice, entity_type_hint, persistence_id, sequence_number, db_timestamp, statement_timestamp() AS read_db_timestamp, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload
       |FROM $journalTable
       |WHERE entity_type_hint = ${nextParam()}
       |AND slice BETWEEN ${nextParam()} AND ${nextParam()}
       |AND db_timestamp >= ${nextParam()} $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
       |AND deleted = false
       |ORDER BY db_timestamp, sequence_number
       |LIMIT ${nextParam()}
       |""".stripMargin
  }

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(ec, system)

  def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.get("db_timestamp", classOf[Instant]))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  def eventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      untilTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration): Source[SerializedJournalRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(eventsBySlicesRangeSql(maxDbTimestampParam = untilTimestamp.isDefined, behindCurrentTime))
          .bind(0, entityTypeHint)
          .bind(1, minSlice)
          .bind(2, maxSlice)
          .bind(3, fromTimestamp)
        untilTimestamp match {
          case Some(until) =>
            stmt.bind(4, until)
            stmt.bind(5, settings.querySettings.bufferSize)
          case None =>
            stmt.bind(4, settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        SerializedJournalRow(
          persistenceId = row.get("persistence_id", classOf[String]),
          sequenceNr = row.get("sequence_number", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
          payload = row.get("event_payload", classOf[Array[Byte]]),
          serId = row.get("event_ser_id", classOf[java.lang.Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = row.get("writer", classOf[String]),
          timestamp = row.get("write_timestamp", classOf[java.lang.Long]),
          tags = Set.empty, // not needed here
          metadata = None // FIXME
        ))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }
}
