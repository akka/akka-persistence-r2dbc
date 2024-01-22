/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.typed.PersistenceId
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.persistence.r2dbc.TestActors.DurableStatePersister

object MigrationToolSpec {
  val config: Config = ConfigFactory
    .parseString("""
    akka-persistence-jdbc {
      shared-databases {
        default {
          profile = "slick.jdbc.PostgresProfile$"
          db {
            host = "127.0.0.1"
            url = "jdbc:postgresql://127.0.0.1:5432/postgres?reWriteBatchedInserts=true"
            user = postgres
            password = postgres
            driver = "org.postgresql.Driver"
            numThreads = 20
            maxConnections = 20
            minConnections = 5
          }
        }
      }
    }

    jdbc-journal {
      use-shared-db = "default"
      tables.event_journal.tableName = "jdbc_event_journal"
    }
    jdbc-snapshot-store {
      use-shared-db = "default"
      tables.snapshot.tableName = "jdbc_snapshot"
    }
    jdbc-read-journal {
      use-shared-db = "default"
      tables.event_journal.tableName = "jdbc_event_journal"
    }
    jdbc-durable-state-store {
      use-shared-db = "default"
      tables.durable_state.tableName = "jdbc_durable_state"
    }

    akka.persistence.r2dbc.state.assert-single-writer = off
    """)
    .withFallback(TestConfig.config)
}

class MigrationToolSpec
    extends ScalaTestWithActorTestKit(MigrationToolSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val migrationConfig = system.settings.config.getConfig("akka.persistence.r2dbc.migration")
  private val sourceJournalPluginId = "jdbc-journal"
  private val sourceSnapshotPluginId = migrationConfig.getString("source.snapshot-plugin-id")
  private val sourceDurableStatePluginId = migrationConfig.getString("source.durable-state-plugin-id")

  private val targetPluginId = migrationConfig.getString("target.persistence-plugin-id")

  private val migration = new MigrationTool(system)

  // enable for sqlserver as well?
  private val testEnabled: Boolean = {
    // don't run this for Yugabyte since it is using akka-persistence-jdbc
    system.settings.config.getString("akka.persistence.r2dbc.connection-factory.dialect") == "postgres"
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (testEnabled) {
      Await.result(
        r2dbcExecutor.executeDdl("beforeAll create jdbc tables") { connection =>
          connection.createStatement("""CREATE TABLE IF NOT EXISTS jdbc_event_journal(
                                       |  ordering BIGSERIAL,
                                       |  persistence_id VARCHAR(255) NOT NULL,
                                       |  sequence_number BIGINT NOT NULL,
                                       |  deleted BOOLEAN DEFAULT FALSE NOT NULL,
                                       |
                                       |  writer VARCHAR(255) NOT NULL,
                                       |  write_timestamp BIGINT,
                                       |  adapter_manifest VARCHAR(255),
                                       |
                                       |  event_ser_id INTEGER NOT NULL,
                                       |  event_ser_manifest VARCHAR(255) NOT NULL,
                                       |  event_payload BYTEA NOT NULL,
                                       |
                                       |  meta_ser_id INTEGER,
                                       |  meta_ser_manifest VARCHAR(255),
                                       |  meta_payload BYTEA,
                                       |
                                       |  PRIMARY KEY(persistence_id, sequence_number)
                                       |)""".stripMargin)
        },
        10.seconds)

      Await.result(
        r2dbcExecutor.executeDdl("beforeAll create jdbc tables") { connection =>
          connection.createStatement("""CREATE TABLE IF NOT EXISTS jdbc_snapshot (
                                       |  persistence_id VARCHAR(255) NOT NULL,
                                       |  sequence_number BIGINT NOT NULL,
                                       |  created BIGINT NOT NULL,
                                       |
                                       |  snapshot_ser_id INTEGER NOT NULL,
                                       |  snapshot_ser_manifest VARCHAR(255) NOT NULL,
                                       |  snapshot_payload BYTEA NOT NULL,
                                       |
                                       |  meta_ser_id INTEGER,
                                       |  meta_ser_manifest VARCHAR(255),
                                       |  meta_payload BYTEA,
                                       |
                                       |  PRIMARY KEY(persistence_id, sequence_number)
                                       |)""".stripMargin)
        },
        10.seconds)

      Await.result(
        r2dbcExecutor.executeDdl("beforeAll create jdbc tables") { connection =>
          connection.createStatement("""CREATE TABLE IF NOT EXISTS jdbc_durable_state (
                                       |  global_offset BIGSERIAL,
                                       |  persistence_id VARCHAR(255) NOT NULL,
                                       |  revision BIGINT NOT NULL,
                                       |  state_payload BYTEA NOT NULL,
                                       |  state_serial_id INTEGER NOT NULL,
                                       |  state_serial_manifest VARCHAR(255),
                                       |  tag VARCHAR,
                                       |  state_timestamp BIGINT NOT NULL,
                                       |  PRIMARY KEY(persistence_id)
                                       |);""".stripMargin)
        },
        10.seconds)

      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete jdbc")(_.createStatement("delete from jdbc_event_journal")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete jdbc")(_.createStatement("delete from jdbc_snapshot")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete jdbc")(_.createStatement("delete from jdbc_durable_state")),
        10.seconds)

      Await.result(migration.migrationDao.createProgressTable(), 10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll migration_progress")(_.createStatement("delete from migration_progress")),
        10.seconds)
    }
  }

  private def persistEvents(pid: PersistenceId, events: Seq[String]): Unit = {
    val probe = testKit.createTestProbe[Done]()
    val persister = testKit.spawn(Persister(pid, sourceJournalPluginId, sourceSnapshotPluginId, tags = Set.empty))
    events.foreach { event =>
      persister ! Persister.Persist(event)
    }
    persister ! Persister.Stop(probe.ref)
    probe.expectMessage(Done)
  }

  private def persistDurableState(pid: PersistenceId, state: Any): Unit = {
    val probe = testKit.createTestProbe[Done]()
    val persister = testKit.spawn(DurableStatePersister(pid, sourceDurableStatePluginId))
    persister ! DurableStatePersister.Persist(state)
    persister ! DurableStatePersister.Stop(probe.ref)
    probe.expectMessage(Done)
  }

  private def assertEvents(pid: PersistenceId, expectedEvents: Seq[String]): Unit =
    assertState(pid, expectedEvents.mkString("|"))

  private def assertState(pid: PersistenceId, expectedState: String): Unit = {
    val probe = testKit.createTestProbe[Any]()
    val targetPersister =
      testKit.spawn(Persister(pid, targetPluginId + ".journal", targetPluginId + ".snapshot", tags = Set.empty))
    targetPersister ! Persister.GetState(probe.ref)
    probe.expectMessage(expectedState)
    targetPersister ! Persister.Stop(probe.ref)
    probe.expectMessage(Done)
  }

  private def assertDurableState(pid: PersistenceId, expectedState: String): Unit = {
    val probe = testKit.createTestProbe[Any]()
    val targetPersister =
      testKit.spawn(DurableStatePersister(pid, targetPluginId + ".state"))
    targetPersister ! DurableStatePersister.GetState(probe.ref)
    probe.expectMessage(expectedState)
    targetPersister ! DurableStatePersister.Stop(probe.ref)
    probe.expectMessage(Done)
  }

  "MigrationTool" should {
    if (!testEnabled) {
      info(
        s"MigrationToolSpec not enabled for ${system.settings.config.getString("akka.persistence.r2dbc.connection-factory.dialect")}")
      pending
    }

    "migrate events of one persistenceId" in {
      val pid = PersistenceId.ofUniqueId(nextPid())

      val events = List("e-1", "e-2", "e-3")
      persistEvents(pid, events)

      migration.migrateEvents(pid.id).futureValue shouldBe 3L

      assertEvents(pid, events)
    }

    "migrate events of a persistenceId several times" in {
      val pid = PersistenceId.ofUniqueId(nextPid())

      val events = List("e-1", "e-2", "e-3")
      persistEvents(pid, events)

      migration.migrateEvents(pid.id).futureValue shouldBe 3L
      assertEvents(pid, events)
      // running again should be idempotent and not fail
      migration.migrateEvents(pid.id).futureValue shouldBe 0L
      assertEvents(pid, events)

      // and running again should find new events
      val moreEvents = List("e-4", "e-5")
      persistEvents(pid, moreEvents)
      migration.migrateEvents(pid.id).futureValue shouldBe 2L

      assertEvents(pid, events ++ moreEvents)
    }

    "migrate snapshot of one persistenceId" in {
      val pid = PersistenceId.ofUniqueId(nextPid())

      persistEvents(pid, List("e-1", "e-2-snap", "e-3"))

      migration.migrateSnapshot(pid.id).futureValue shouldBe 1L

      assertState(pid, "e-1|e-2-snap")
    }

    "migrate snapshot of a persistenceId several times" in {
      val pid = PersistenceId.ofUniqueId(nextPid())

      persistEvents(pid, List("e-1", "e-2-snap", "e-3"))

      migration.migrateSnapshot(pid.id).futureValue shouldBe 1L
      assertState(pid, "e-1|e-2-snap")
      // running again should be idempotent and not fail
      migration.migrateSnapshot(pid.id).futureValue shouldBe 0L
      assertState(pid, "e-1|e-2-snap")

      // and running again should find new snapshot
      persistEvents(pid, List("e-4-snap", "e-5"))
      migration.migrateSnapshot(pid.id).futureValue shouldBe 1L

      assertState(pid, "e-1|e-2-snap|e-3|e-4-snap")
    }

    "update event migration_progress" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.eventSeqNr) shouldBe None

      persistEvents(pid, List("e-1", "e-2", "e-3"))
      migration.migrateEvents(pid.id).futureValue shouldBe 3L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.eventSeqNr) shouldBe Some(3L)

      // store and migration some more
      persistEvents(pid, List("e-4", "e-5"))
      migration.migrateEvents(pid.id).futureValue shouldBe 2L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.eventSeqNr) shouldBe Some(5L)
    }

    "update snapshot migration_progress" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.snapshotSeqNr) shouldBe None

      persistEvents(pid, List("e-1", "e-2-snap", "e-3"))
      migration.migrateSnapshot(pid.id).futureValue shouldBe 1L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.snapshotSeqNr) shouldBe Some(2L)

      // store and migration some more
      persistEvents(pid, List("e-4", "e-5-snap", "e-6"))
      migration.migrateSnapshot(pid.id).futureValue shouldBe 1L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.snapshotSeqNr) shouldBe Some(5L)
    }

    "migrate all persistenceIds" in {
      val numberOfPids = 10
      val pids = (1 to numberOfPids).map(_ => PersistenceId.ofUniqueId(nextPid()))
      val events = List("e-1", "e-2", "e-3", "e-4-snap", "e-5", "e-6-snap", "e-7", "e-8", "e-9")

      pids.foreach { pid =>
        persistEvents(pid, events)
      }

      migration.migrateAll().futureValue

      pids.foreach { pid =>
        assertEvents(pid, events)
      }
    }

    "migrate durable state of one persistenceId" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      persistDurableState(pid, "s-1")
      migration.migrateDurableState(pid.id).futureValue shouldBe 1L
      assertDurableState(pid, "s-1")
    }

    "migrate durable state of a persistenceId several times" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      persistDurableState(pid, "s-1")
      migration.migrateDurableState(pid.id).futureValue shouldBe 1L
      assertDurableState(pid, "s-1")

      // running again should be idempotent and not fail
      migration.migrateDurableState(pid.id).futureValue shouldBe 0L
      assertDurableState(pid, "s-1")

      // and running again should find updated revision
      persistDurableState(pid, "s-2")
      migration.migrateDurableState(pid.id).futureValue shouldBe 1L
      assertDurableState(pid, "s-2")
    }

    "update durable state migration_progress" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.durableStateRevision) shouldBe None

      persistDurableState(pid, "s-1")
      migration.migrateDurableState(pid.id).futureValue shouldBe 1L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.durableStateRevision) shouldBe Some(1L)

      // store and migration some more
      persistDurableState(pid, "s-2")
      persistDurableState(pid, "s-3")
      migration.migrateDurableState(pid.id).futureValue shouldBe 1L
      migration.migrationDao.currentProgress(pid.id).futureValue.map(_.durableStateRevision) shouldBe Some(3L)
    }

    "migrate all durable state persistenceIds" in {
      val numberOfPids = 10
      val pids = (1 to numberOfPids).map(_ => PersistenceId.ofUniqueId(nextPid()))

      pids.foreach { pid =>
        persistDurableState(pid, s"s-$pid")
      }

      migration.migrateDurableStates(pids.map(_.id)).futureValue

      pids.foreach { pid =>
        assertDurableState(pid, s"s-$pid")
      }
    }

  }
}
