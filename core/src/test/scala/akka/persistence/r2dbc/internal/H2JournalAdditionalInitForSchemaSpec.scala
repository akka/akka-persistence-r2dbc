/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn.BindValue
import akka.persistence.typed.PersistenceId

object H2JournalAdditionalInitForSchemaSpec {

  class TitleColumn extends AdditionalColumn[String, String] {
    override def columnName: String = "title"

    override def bind(insert: AdditionalColumn.Insert[String]): AdditionalColumn.Binding[String] =
      BindValue(insert.value)
  }

  val conf = ConfigFactory
    .parseString(s"""
      akka.loglevel = DEBUG
      akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
      akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
      akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      akka.actor.testkit.typed.default-timeout = 10s

      akka.persistence.r2dbc.journal {
        additional-columns {
           "CustomJournalEntity" = ["${classOf[TitleColumn].getName}"]
         }
      }
      // #additionalColumnJournal
      akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
      akka.persistence.r2dbc.connection-factory {
        protocol = "mem"
        database = "h2-journal-additional-init-db"
        additional-init = "alter table event_journal add if not exists title varchar(256)"
      }
      // #additionalColumnJournal

      # when testing with number-of-databases > 1 we must override that for H2
      akka.persistence.r2dbc.data-partition.number-of-databases = 1
      """)
    .withFallback(ConfigFactory.load())
    .resolve()
}

class H2JournalAdditionalInitForSchemaSpec
    extends ScalaTestWithActorTestKit(H2JournalAdditionalInitForSchemaSpec.conf)
    with TestDbLifecycle
    with TestData
    with AnyWordSpecLike
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private def journalTable(slice: Int) = settings.getJournalTableWithSchema("CustomJournalEntity", slice)

  private def countTitleMatch(persistenceId: String, title: String): Long = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    r2dbcExecutor(slice)
      .selectOne("count title")(
        _.createStatement(
          s"select count(*) from ${journalTable(slice)} where persistence_id = '$persistenceId' and title = '$title'"),
        row => row.get(0, classOf[java.lang.Long]).longValue())
      .futureValue
      .getOrElse(0L)
  }

  "The R2DBC event journal with an additional column created via H2 additional-init" should {

    pendingIfMoreThanOneDataPartition()

    "store an event and populate its additional column" in {
      val entityType = "CustomJournalEntity"
      val persistenceId = PersistenceId.ofUniqueId(nextPid(entityType))
      val ref = spawn(Persister(persistenceId))

      val ack = createTestProbe[Done]()
      ref ! Persister.PersistWithAck("first-title", ack.ref)
      ack.receiveMessage(10.seconds)

      countTitleMatch(persistenceId.id, "first-title") shouldBe 1L
    }
  }
}
