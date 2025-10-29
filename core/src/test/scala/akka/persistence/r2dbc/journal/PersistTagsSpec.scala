/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.journal

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.codec.TagsCodec
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.typed.PersistenceId

class PersistTagsSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  implicit val codec: TagsCodec = settings.codecSettings.JournalImplicits.tagsCodec
  case class Row(pid: String, seqNr: Long, tags: Set[String])

  private def selectRows(table: String, minSlice: Int): IndexedSeq[Row] = {
    r2dbcExecutor(minSlice)
      .select[Row]("test")(
        connection => connection.createStatement(s"select * from $table"),
        row =>
          Row(
            pid = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            row.getTags("tags")))
      .futureValue
  }

  private def selectAllRows(): IndexedSeq[Row] =
    settings.allJournalTablesWithSchema.toVector.sortBy(_._1).flatMap { case (table, minSlice) =>
      selectRows(table, minSlice)
    }

  "Persist tags" should {

    "be the same for events stored in same transaction" in {
      val numberOfEntities = 9
      val entityType = nextEntityType()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityType, s"p$n")
        val tags = Set(entityType, s"tag-p$n")
        spawn(Persister(persistenceId, tags), s"p$n")
      }

      entities.foreach { ref =>
        ref ! Persister.Persist("e1")
      }

      val pingProbe = createTestProbe[Done]()
      entities.foreach { ref =>
        ref ! Persister.Ping(pingProbe.ref)
      }
      pingProbe.receiveMessages(entities.size, 20.seconds)

      val rows = selectAllRows()

      rows.foreach { case Row(pid, _, tags) =>
        withClue(s"pid [$pid}]: ") {
          tags shouldBe Set(PersistenceId.extractEntityType(pid), s"tag-${PersistenceId.extractEntityId(pid)}")
        }
      }
    }

  }
}
