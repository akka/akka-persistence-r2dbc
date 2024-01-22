/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresSnapshotDao
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext

import io.r2dbc.spi.Row

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class H2SnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresSnapshotDao(settings, connectionFactory) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2SnapshotDao])

  override protected def createUpsertSql: String = {
    // db_timestamp and tags columns were added in 1.2.0
    if (settings.querySettings.startFromSnapshotEnabled)
      sql"""
      MERGE INTO $snapshotTable
      (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, db_timestamp, tags)
      KEY (persistence_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
    else
      sql"""
      MERGE INTO $snapshotTable
      (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)
      KEY (persistence_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
  }
}
