/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.postgres.PostgresSnapshotDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class H2SnapshotDao(executorProvider: R2dbcExecutorProvider)
    extends PostgresSnapshotDao(executorProvider) {
  import settings.codecSettings.SnapshotImplicits._

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2SnapshotDao])

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  override protected def upsertSql(slice: Int): String =
    sqlCache.get(slice, "upsertSql") {
      // db_timestamp and tags columns were added in 1.2.0
      if (settings.querySettings.startFromSnapshotEnabled)
        sql"""
        MERGE INTO ${snapshotTable(slice)}
        (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, db_timestamp, tags)
        KEY (persistence_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
      else
        sql"""
        MERGE INTO ${snapshotTable(slice)}
        (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)
        KEY (persistence_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    }
}
