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
import akka.persistence.r2dbc.internal.{
  AdditionalColumnFactory,
  ChangeHandlerFactory,
  Dialect,
  DurableStateDao,
  InstantFactory,
  PayloadCodec,
  R2dbcExecutor,
  TimestampCodec
}
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichRow => TimestampRichRow }
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichStatement => TimestampRichStatement }
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.r2dbc.internal.postgres.sql.{ BaseDurableStateSql, PostgresDurableStateSql }
import akka.persistence.r2dbc.internal.sqlserver.sql.SqlServerDurableStateSql
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
private[r2dbc] class SqlServerDurableStateDao(
    settings: R2dbcSettings,
    connectionFactory: ConnectionFactory,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, connectionFactory, dialect) {

  override def log: Logger = SqlServerDurableStateDao.log

  protected override implicit val statePayloadCodec: PayloadCodec = settings.durableStatePayloadCodec
  protected override implicit val timestampCodec: TimestampCodec = settings.timestampCodec

  override val durableStateSql: BaseDurableStateSql = new SqlServerDurableStateSql(settings)

  override def currentDbTimestamp(): Future[Instant] = Future.successful(timestampCodec.now())

}
