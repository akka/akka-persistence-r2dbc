/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.h2.H2Dialect

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.persistence.r2dbc"

  lazy val r2dbcSettings: R2dbcSettings =
    new R2dbcSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(testConfigPath + ".connection-factory"),
      LoggerFactory.getLogger(getClass),
      r2dbcSettings.logDbCallsExceeding)(typedSystem.executionContext, typedSystem)
  }

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  override protected def beforeAll(): Unit = {
    if (r2dbcSettings.dialect == H2Dialect)
      createH2Tables()
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${r2dbcSettings.journalTableWithSchema}")),
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${r2dbcSettings.snapshotsTableWithSchema}")),
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${r2dbcSettings.durableStateTableWithSchema}")),
      10.seconds)
    super.beforeAll()
  }

  private def createH2Tables(): Unit = {
    Await.result(
      r2dbcExecutor.executeDdl("create journal table")(_.createStatement(sql"""
          CREATE TABLE IF NOT EXISTS ${r2dbcSettings.journalTableWithSchema}(
            slice INT NOT NULL,
            entity_type VARCHAR(255) NOT NULL,
            persistence_id VARCHAR(255) NOT NULL,
            seq_nr BIGINT NOT NULL,
            db_timestamp timestamp with time zone NOT NULL,

            event_ser_id INTEGER NOT NULL,
            event_ser_manifest VARCHAR(255) NOT NULL,
            event_payload BYTEA NOT NULL,

            deleted BOOLEAN DEFAULT FALSE NOT NULL,
            writer VARCHAR(255) NOT NULL,
            adapter_manifest VARCHAR(255),
            tags TEXT ARRAY,

            meta_ser_id INTEGER,
            meta_ser_manifest VARCHAR(255),
            meta_payload BYTEA,

            PRIMARY KEY(persistence_id, seq_nr)
          );
        """)),
      10.seconds)
    Await
      .result(
        r2dbcExecutor.executeDdl("create snapshot table")(_.createStatement(sql"""
          CREATE TABLE IF NOT EXISTS ${r2dbcSettings.snapshotsTableWithSchema}(
            slice INT NOT NULL,
            entity_type VARCHAR(255) NOT NULL,
            persistence_id VARCHAR(255) NOT NULL,
            seq_nr BIGINT NOT NULL,
            write_timestamp BIGINT NOT NULL,
            ser_id INTEGER NOT NULL,
            ser_manifest VARCHAR(255) NOT NULL,
            snapshot BYTEA NOT NULL,
            meta_ser_id INTEGER,
            meta_ser_manifest VARCHAR(255),
            meta_payload BYTEA,

            PRIMARY KEY(persistence_id)
          );
        """)),
        10.seconds)
    Await
      .result(
        r2dbcExecutor.executeDdl("create snapshot table")(_.createStatement(sql"""
          CREATE TABLE IF NOT EXISTS ${r2dbcSettings.durableStateTableWithSchema} (
            slice INT NOT NULL,
            entity_type VARCHAR(255) NOT NULL,
            persistence_id VARCHAR(255) NOT NULL,
            revision BIGINT NOT NULL,
            db_timestamp timestamp with time zone NOT NULL,

            state_ser_id INTEGER NOT NULL,
            state_ser_manifest VARCHAR(255),
            state_payload BYTEA NOT NULL,
            tags TEXT ARRAY,

            PRIMARY KEY(persistence_id, revision)
          );
        """)),
        10.seconds)

  }

}
