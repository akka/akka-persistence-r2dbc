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
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn.BindNull
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn.BindValue
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn.Skip
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink

object JournalAdditionalColumnSpec {
  val EntityType = "CustomJournalColEntity"

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.persistence.r2dbc.journal {
      additional-columns {
        "$EntityType" = ["${classOf[TitleColumn].getName}"]
      }
    }
    """)
    .withFallback(TestConfig.config)

  // String -> String column on the default event_journal table
  class TitleColumn extends AdditionalColumn[String, String] {
    override def columnName: String = "col_title"

    override def bind(insert: AdditionalColumn.Insert[String]): AdditionalColumn.Binding[String] =
      insert.value match {
        case ""     => BindNull
        case "SKIP" => Skip
        case s      => BindValue(s)
      }
  }
}

class JournalAdditionalColumnSpec
    extends ScalaTestWithActorTestKit(JournalAdditionalColumnSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import JournalAdditionalColumnSpec.EntityType

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  // The additional column is added to the (shared) default event_journal table. It is nullable, so writes for other
  // entity types that don't define the column are unaffected.
  private def journalTable(slice: Int) = settings.journalTableWithSchema(slice)

  private def alterAddColumn(slice: Int, col: String, colType: String): String =
    if (settings.dialectName == "sqlserver")
      s"IF COL_LENGTH('${journalTable(slice)}', '$col') IS NULL ALTER TABLE ${journalTable(slice)} ADD $col $colType"
    else
      s"alter table ${journalTable(slice)} add if not exists $col $colType"

  override def beforeAll(): Unit = {
    settings.dataPartitionSliceRanges.foreach { sliceRange =>
      val slice = sliceRange.min
      Await.result(
        r2dbcExecutor(slice).executeDdl("beforeAll alter event_journal add col_title")(
          _.createStatement(alterAddColumn(slice, "col_title", "varchar(256)"))),
        20.seconds)
    }
    super.beforeAll()
  }

  private def selectColTitle(persistenceId: String, seqNr: Long): Option[String] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    r2dbcExecutor(slice)
      .selectOne[Option[String]]("test")(
        _.createStatement(
          s"select col_title from ${journalTable(slice)} where persistence_id = '$persistenceId' and seq_nr = $seqNr"),
        row => Option(row.get("col_title", classOf[String])))
      .futureValue
      .flatten
  }

  private def persist(persistenceId: PersistenceId, payload: String): Unit = {
    val ref = spawn(Persister(persistenceId))
    val ack = createTestProbe[Done]()
    ref ! Persister.PersistWithAck(payload, ack.ref)
    ack.receiveMessage(10.seconds)
  }

  "The R2DBC journal with additional columns" should {

    "store an event and populate the additional column" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      persist(persistenceId, "the-title")

      selectColTitle(persistenceId.id, 1L) shouldBe Some("the-title")

      // the event is readable through the query API
      val events =
        query.currentEventsByPersistenceId(persistenceId.id, 0, Long.MaxValue).runWith(Sink.seq).futureValue
      events.map(_.event) shouldBe Seq("the-title")
    }

    "support null binding of the additional column" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      persist(persistenceId, "")

      selectColTitle(persistenceId.id, 1L) shouldBe None
    }

    "populate the additional column for every event of a batch" in {
      val persistenceId = PersistenceId.ofUniqueId(nextPid(EntityType))
      val ref = spawn(Persister(persistenceId))
      val ack = createTestProbe[Done]()
      ref ! Persister.PersistAll(List("a", "b", "c"))
      ref ! Persister.PersistWithAck("d", ack.ref)
      ack.receiveMessage(10.seconds)

      selectColTitle(persistenceId.id, 1L) shouldBe Some("a")
      selectColTitle(persistenceId.id, 2L) shouldBe Some("b")
      selectColTitle(persistenceId.id, 3L) shouldBe Some("c")
      selectColTitle(persistenceId.id, 4L) shouldBe Some("d")
    }

    "not affect events of other entity types without the column configured" in {
      val otherPid = PersistenceId.ofUniqueId(nextPid(nextEntityType()))
      persist(otherPid, "no-column-here")

      // col_title is left NULL for entity types without additional-columns configured
      selectColTitle(otherPid.id, 1L) shouldBe None
      val events = query.currentEventsByPersistenceId(otherPid.id, 0, Long.MaxValue).runWith(Sink.seq).futureValue
      events.map(_.event) shouldBe Seq("no-column-here")
    }
  }
}
