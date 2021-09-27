/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
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
    system: ActorSystem[_]) {
  import QueryDao.log

  private val currentDbTimestampSql =
    "SELECT transaction_timestamp() AS db_timestamp"

  private val behindCurrentTimeInterval =
    s"${settings.querySettings.behindCurrentTime.toMillis} milliseconds"

  private def eventsBySlicesRangeSql(maxDbTimestamp: Boolean): String =
    // FIXME make that interval configurable
    s"""SELECT *
       |FROM ${settings.journalTable}
       |WHERE entity_type_hint = $$1
       |AND slice BETWEEN $$2 AND $$3
       |AND db_timestamp >= $$4 ${if (maxDbTimestamp) "AND db_timestamp <= $6"
    else s"AND db_timestamp < statement_timestamp() - interval '$behindCurrentTimeInterval'"}
       |AND deleted = false
       |ORDER BY db_timestamp, sequence_number
       |LIMIT $$5
       |""".stripMargin

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
      toTimestamp: Option[Instant]): Source[SerializedJournalRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(eventsBySlicesRangeSql(toTimestamp.isDefined))
          .bind("$1", entityTypeHint)
          .bind("$2", minSlice)
          .bind("$3", maxSlice)
          .bind("$4", fromTimestamp)
        toTimestamp match {
          case Some(to) =>
            stmt.bind("$5", settings.querySettings.bufferSize)
            stmt.bind("$6", to)
          case None =>
            stmt.bind("$5", settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        SerializedJournalRow(
          persistenceId = row.get("persistence_id", classOf[String]),
          sequenceNr = row.get("sequence_number", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
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
