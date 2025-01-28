/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.R2dbcExecutor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import java.time.Instant

import scala.util.control.NonFatal

import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.PayloadCodec
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>
  private val log = LoggerFactory.getLogger(getClass)

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.persistence.r2dbc"

  lazy val settings: R2dbcSettings =
    R2dbcSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val r2dbcExecutorProvider: R2dbcExecutorProvider =
    new R2dbcExecutorProvider(
      typedSystem,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, typedSystem),
      settings,
      testConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))

  def r2dbcExecutor(slice: Int): R2dbcExecutor =
    r2dbcExecutorProvider.executorFor(slice)

  lazy val r2dbcExecutor: R2dbcExecutor =
    r2dbcExecutor(slice = 0)

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  def pendingIfMoreThanOneDataPartition(): Unit =
    if (settings.numberOfDataPartitions > 1)
      pending

  override protected def beforeAll(): Unit = {
    try {
      settings.allJournalTablesWithSchema.foreach { case (table, minSlice) =>
        Await.result(
          r2dbcExecutor(minSlice).updateOne("beforeAll delete")(_.createStatement(s"delete from $table")),
          10.seconds)
      }
      settings.allSnapshotTablesWithSchema.foreach { case (table, minSlice) =>
        Await.result(
          r2dbcExecutor(minSlice).updateOne("beforeAll delete")(_.createStatement(s"delete from $table")),
          10.seconds)
      }
      settings.allDurableStateTablesWithSchema.foreach { case (table, minSlice) =>
        Await.result(
          r2dbcExecutor(minSlice).updateOne("beforeAll delete")(_.createStatement(s"delete from $table")),
          10.seconds)
      }
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Test db creation failed", ex)
    }
    super.beforeAll()
  }

  // to be able to store events with specific timestamps
  def writeEvent(slice: Int, persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    implicit val timestampCodec: TimestampCodec = settings.codecSettings.JournalImplicits.timestampCodec
    implicit val payloadCodec: PayloadCodec = settings.codecSettings.JournalImplicits.journalPayloadCodec
    implicit val queryAdapter: QueryAdapter = settings.codecSettings.JournalImplicits.queryAdapter
    import TimestampCodec.TimestampCodecRichStatement
    import PayloadCodec.RichStatement
    import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
    val stringSerializer = SerializationExtension(typedSystem).serializerFor(classOf[String])

    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${settings.journalTableWithSchema(slice)}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload)
      VALUES (?, ?, ?, ?, ?, '', '', ?, '', ?)"""
    val entityType = PersistenceId.extractEntityType(persistenceId)

    val result = r2dbcExecutor(slice).updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, seqNr)
        .bindTimestamp(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bindPayload(6, stringSerializer.toBinary(event))
    }
    Await.result(result, 5.seconds)
  }

}
