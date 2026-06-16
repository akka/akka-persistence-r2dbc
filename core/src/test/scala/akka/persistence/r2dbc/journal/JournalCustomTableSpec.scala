/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.journal

import java.lang

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink

object JournalCustomTableSpec {
  val EntityType = "CustomTableJournalEntity"
  val CustomTable = "event_journal_custom"

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.persistence.r2dbc.journal {
      custom-table {
        "$EntityType" = $CustomTable
      }
    }
    """)
    .withFallback(TestConfig.config)
}

class JournalCustomTableSpec
    extends ScalaTestWithActorTestKit(JournalCustomTableSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import JournalCustomTableSpec.EntityType

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private def customTable(slice: Int) = settings.getJournalTableWithSchema(EntityType, slice)
  private def defaultTable(slice: Int) = settings.journalTableWithSchema(slice)

  // The custom journal table must have the same layout as the default event_journal table, including the
  // `deleted BOOLEAN DEFAULT FALSE` column that the insert relies on (the insert does not list `deleted`).
  // Create-as-select (like the durable state spec) doesn't copy column defaults, so restore the `deleted` default
  // afterwards. Avoids `LIKE ... INCLUDING DEFAULTS`, which Yugabyte does not support.
  private def createCustomTableStatements(slice: Int): Seq[String] = settings.dialectName match {
    case "sqlserver" =>
      Seq(
        s"IF object_id('${customTable(slice)}') is null SELECT * INTO ${customTable(slice)} FROM ${defaultTable(slice)} WHERE persistence_id = ''",
        s"IF NOT EXISTS (select 1 from sys.default_constraints where name = 'df_${EntityType}_deleted') " +
        s"ALTER TABLE ${customTable(slice)} ADD CONSTRAINT df_${EntityType}_deleted DEFAULT 0 FOR deleted")
    case _ =>
      // postgres, yugabyte and h2
      Seq(
        s"create table if not exists ${customTable(slice)} as select * from ${defaultTable(slice)} where persistence_id = ''",
        s"alter table ${customTable(slice)} alter column deleted set default false")
  }

  override def beforeAll(): Unit = {
    // create the custom table before super.beforeAll(), which deletes from all journal tables (incl. the custom one)
    settings.dataPartitionSliceRanges.foreach { sliceRange =>
      val slice = sliceRange.min
      createCustomTableStatements(slice).foreach { ddl =>
        Await.result(
          r2dbcExecutor(slice).executeDdl("beforeAll create custom journal table")(_.createStatement(ddl)),
          20.seconds)
      }
    }
    super.beforeAll()
  }

  private def countIn(table: String, slice: Int, persistenceId: String): Long =
    r2dbcExecutor(slice)
      .selectOne[Long]("count")(
        _.createStatement(s"select count(*) from $table where persistence_id = '$persistenceId'"),
        row => row.get(0, classOf[lang.Long]).longValue())
      .futureValue
      .getOrElse(0L)

  private def persist(persistenceId: PersistenceId, payload: String): Unit = {
    val ref = spawn(Persister(persistenceId))
    val ack = createTestProbe[Done]()
    ref ! Persister.PersistWithAck(payload, ack.ref)
    ack.receiveMessage(10.seconds)
  }

  "The R2DBC journal with a custom table per entity type" should {

    pendingIfMoreThanOneDataPartition()

    "write events to the custom table for the configured entity type" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      val slice = persistenceExt.sliceForPersistenceId(persistenceId.id)
      persist(persistenceId, "only-event")

      countIn(customTable(slice), slice, persistenceId.id) shouldBe 1L
      countIn(defaultTable(slice), slice, persistenceId.id) shouldBe 0L
    }

    "write events for an unconfigured entity type to the default table" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(nextEntityType()))
      val slice = persistenceExt.sliceForPersistenceId(persistenceId.id)
      persist(persistenceId, "only-event")

      countIn(defaultTable(slice), slice, persistenceId.id) shouldBe 1L
      countIn(customTable(slice), slice, persistenceId.id) shouldBe 0L
    }

    "read events from the custom table via eventsByPersistenceId" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      val ref = spawn(Persister(persistenceId))
      val ack = createTestProbe[Done]()
      ref ! Persister.PersistAll(List("e1", "e2"))
      ref ! Persister.PersistWithAck("e3", ack.ref)
      ack.receiveMessage(10.seconds)

      val events =
        query.currentEventsByPersistenceId(persistenceId.id, 0, Long.MaxValue).runWith(Sink.seq).futureValue
      events.map(_.event) shouldBe Seq("e1", "e2", "e3")
    }

    "read events from the custom table via eventsBySlices" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      val slice = query.sliceForPersistenceId(persistenceId.id)
      persist(persistenceId, "sliced")

      val events =
        query
          .currentEventsBySlices[String](EntityType, slice, slice, NoOffset)
          .runWith(Sink.seq)
          .futureValue
      events.map(_.persistenceId) should contain(persistenceId.id)
    }

    "return persistence ids spanning the default and custom tables" in {
      val customPid = PersistenceId.ofUniqueId(nextPid(EntityType))
      val defaultPid = PersistenceId.ofUniqueId(nextPid(nextEntityType()))
      persist(customPid, "in-custom")
      persist(defaultPid, "in-default")

      val ids = query.currentPersistenceIds().runWith(Sink.seq).futureValue
      ids should contain(customPid.id)
      ids should contain(defaultPid.id)
    }
  }
}
