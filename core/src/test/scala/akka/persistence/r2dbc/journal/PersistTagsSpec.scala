/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.codec.TagsCodec
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.typed.PersistenceId
import org.scalatest.wordspec.AnyWordSpecLike

class PersistTagsSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val settings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  implicit val tagsCodec: TagsCodec = settings.tagsCodec
  case class Row(pid: String, seqNr: Long, tags: Set[String])

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

      val rows =
        r2dbcExecutor
          .select[Row]("test")(
            connection => connection.createStatement(s"select * from ${settings.journalTableWithSchema}"),
            row =>
              Row(
                pid = row.get("persistence_id", classOf[String]),
                seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
                row.getTags()))
          .futureValue

      rows.foreach { case Row(pid, _, tags) =>
        withClue(s"pid [$pid}]: ") {
          tags shouldBe Set(PersistenceId.extractEntityType(pid), s"tag-${PersistenceId.extractEntityId(pid)}")
        }
      }
    }

  }
}
