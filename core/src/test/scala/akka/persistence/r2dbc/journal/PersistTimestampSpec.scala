/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import java.time.Instant

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
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import org.scalatest.wordspec.AnyWordSpecLike

class PersistTimestampSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val settings = new R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  private val serialization = SerializationExtension(system)

  case class Row(pid: String, seqNr: Long, dbTimestamp: Instant, writeTimestamp: Long, event: String)

  "Persist timestamp" should {

    "be the same for events stored in same transaction" in {
      val numberOfEntities = 20
      val entityTypeHint = nextEntityTypeHint()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityTypeHint, s"p$n")
        spawn(Persister(persistenceId), s"p$n")
      }

      (1 to 100).foreach { n =>
        val p = n % numberOfEntities
        // mix some persist 1 and persist 3 events
        if (n % 5 == 0) {
          // same event stored 3 times
          val event = s"e$p-$n"
          entities(p) ! Persister.PersistAll((0 until 3).map(_ => event).toList)
        } else {
          entities(p) ! Persister.Persist(s"e$p-$n")
        }
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
            row => {
              val event = serialization
                .deserialize(
                  row.get("event_payload", classOf[Array[Byte]]),
                  row.get("event_ser_id", classOf[Integer]),
                  row.get("event_ser_manifest", classOf[String]))
                .get
                .asInstanceOf[String]
              Row(
                pid = row.get("persistence_id", classOf[String]),
                seqNr = row.get("sequence_number", classOf[java.lang.Long]),
                dbTimestamp = row.get("db_timestamp", classOf[Instant]),
                writeTimestamp = row.get("write_timestamp", classOf[java.lang.Long]),
                event)
            })
          .futureValue

      rows.groupBy(_.event).foreach { case (_, rowsByUniqueEvent) =>
        withClue(s"pid [${rowsByUniqueEvent.head.pid}]: ") {
          rowsByUniqueEvent.map(_.dbTimestamp).toSet shouldBe Set(rowsByUniqueEvent.head.dbTimestamp)
          rowsByUniqueEvent.map(_.writeTimestamp).toSet shouldBe Set(rowsByUniqueEvent.head.writeTimestamp)
        }
      }

      val rowOrdering: Ordering[Row] = Ordering.fromLessThan[Row] { (a, b) =>
        if (a eq b) false
        else if (a.dbTimestamp != b.dbTimestamp) a.dbTimestamp.compareTo(b.dbTimestamp) < 0
        else a.seqNr.compareTo(b.seqNr) < 0
      }

      rows.groupBy(_.pid).foreach { case (_, rowsByPid) =>
        withClue(s"pid [${rowsByPid.head.pid}]: ") {
          rowsByPid.sortBy(_.seqNr) shouldBe rowsByPid.sorted(rowOrdering)
        }
      }
    }

  }
}
