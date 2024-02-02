/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.nio.charset.StandardCharsets.UTF_8

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors.DurableStatePersister
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichRow
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.persistence.r2dbc.internal.codec.PayloadCodec

/**
 * The purpose of this test is to verify JSONB payloads, but it can also be run with ordinary BYTEA payloads. To test
 * with JSONB the db schema should be created with `ddl-scripts/create_tables_postgres_jsonb.sql` and start `sbt` with
 *
 * {{{
 * sbt -Dakka.persistence.r2dbc.journal.payload-column-type=JSONB -Dakka.persistence.r2dbc.snapshot.payload-column-type=JSONB -Dakka.persistence.r2dbc.state.payload-column-type=JSONB
 * }}}
 *
 * Note that other test may fail with JSONB column type because the test data isn't in json.
 */
object PayloadSpec {
  val config = ConfigFactory
    .parseString("""
    akka.serialization.jackson.jackson-json.compression.algorithm = off
    """)
    .withFallback(TestConfig.config)

  final case class TestRow(pid: String, seqNr: Long, payload: Array[Byte])

  final case class TestJson(a: String, i: Int) extends JsonSerializable

  implicit class JsonString(val s: String) extends AnyVal {
    // don't care about json formatting, which may be different for JSONB and BYTEA
    def clearWhitespace: String = {
      val sb = new StringBuilder(s.length)
      s.foreach { ch =>
        if (!ch.isWhitespace)
          sb.append(ch)
      }
      sb.result()
    }
  }
}

class PayloadSpec
    extends ScalaTestWithActorTestKit(PayloadSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import PayloadSpec._

  override def typedSystem: ActorSystem[_] = system
  private val settings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  private def testJournalPersister(persistenceId: String, msg: Any): Unit = {
    val probe = createTestProbe[Any]()
    val ref1 = spawn(Persister(persistenceId))
    ref1 ! Persister.PersistWithAck(msg, probe.ref)
    probe.expectMessage(Done)
    testKit.stop(ref1)

    val ref2 = spawn(Persister(persistenceId))
    ref2 ! Persister.GetState(probe.ref)
    probe.receiveMessage().toString.clearWhitespace shouldBe msg.toString.clearWhitespace
    testKit.stop(ref2)
  }

  private def testDurableStatePersister(persistenceId: String, msg: Any): Unit = {
    val probe = createTestProbe[Any]()
    val ref1 = spawn(DurableStatePersister(persistenceId))
    ref1 ! DurableStatePersister.PersistWithAck(msg, probe.ref)
    probe.expectMessage(Done)
    testKit.stop(ref1)

    val ref2 = spawn(DurableStatePersister(persistenceId))
    ref2 ! DurableStatePersister.GetState(probe.ref)
    probe.receiveMessage().toString.clearWhitespace shouldBe msg.toString.clearWhitespace
    testKit.stop(ref2)
  }

  private def selectJournalRow(persistenceId: String): TestRow = {
    implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec

    r2dbcExecutor
      .selectOne[TestRow]("test")(
        connection =>
          connection.createStatement(
            s"select * from ${settings.journalTableWithSchema} where persistence_id = '$persistenceId'"),
        row => {
          val payload = row.getPayload("event_payload")
          TestRow(
            pid = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            payload)
        })
      .futureValue
      .get
  }

  private def selectSnapshotRow(persistenceId: String): TestRow = {
    implicit val snapshotPayloadCodec: PayloadCodec = settings.snapshotPayloadCodec

    r2dbcExecutor
      .selectOne[TestRow]("test")(
        connection =>
          connection.createStatement(
            s"select * from ${settings.snapshotsTableWithSchema} where persistence_id = '$persistenceId'"),
        row => {
          val payload = row.getPayload("snapshot")
          TestRow(
            pid = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            payload)
        })
      .futureValue
      .get
  }

  private def selectDurableStateRow(persistenceId: String): TestRow = {
    implicit val durableStatePayloadCodec: PayloadCodec = settings.durableStatePayloadCodec

    r2dbcExecutor
      .selectOne[TestRow]("test")(
        connection =>
          connection.createStatement(
            s"select * from ${settings.durableStateTableWithSchema} where persistence_id = '$persistenceId'"),
        row => {
          val payload = row.getPayload("state_payload")
          TestRow(
            pid = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("revision", classOf[java.lang.Long]),
            payload)
        })
      .futureValue
      .get
  }

  "journal" should {
    "store json string payload" in {
      val persistenceId = nextPid(nextEntityType())
      val msg = """{"a": "b"}"""
      testJournalPersister(persistenceId, msg)

      val row = selectJournalRow(persistenceId)
      new String(row.payload, UTF_8) shouldBe msg
    }

    "store serialized json payload" in {
      val persistenceId = nextPid(nextEntityType())
      val msg = TestJson("b", 17)
      testJournalPersister(persistenceId, msg)

      val row = selectJournalRow(persistenceId)
      new String(row.payload, UTF_8).clearWhitespace shouldBe """{"a": "b", "i": 17}""".clearWhitespace
    }
  }

  "snapshot store" should {
    "store json string payload" in {
      val persistenceId = nextPid(nextEntityType())
      val msg = """{"a": "snap"}"""
      testJournalPersister(persistenceId, msg)

      val row = selectSnapshotRow(persistenceId)
      new String(row.payload, UTF_8).clearWhitespace shouldBe msg.clearWhitespace
    }
  }

  "durable state" should {
    "store json string payload" in {
      val persistenceId = nextPid(nextEntityType())
      val msg = """{"a": "b"}"""
      testDurableStatePersister(persistenceId, msg)

      val row = selectDurableStateRow(persistenceId)
      new String(row.payload, UTF_8).clearWhitespace shouldBe msg.clearWhitespace
    }

    "store serialized json payload" in {
      val persistenceId = nextPid(nextEntityType())
      val msg = TestJson("b", 17)
      testDurableStatePersister(persistenceId, msg)

      val row = selectDurableStateRow(persistenceId)
      new String(row.payload, UTF_8).clearWhitespace shouldBe """{"a": "b", "i": 17}""".clearWhitespace
    }

    "store delete marker" in {
      val persistenceId = nextPid(nextEntityType())
      val probe = createTestProbe[Any]()
      val ref1 = spawn(DurableStatePersister(persistenceId))
      // delete before any change should insert delete marker
      ref1 ! DurableStatePersister.DeleteWithAck(probe.ref)
      probe.expectMessage(Done)
      testKit.stop(ref1)

      val row1 = selectDurableStateRow(persistenceId)
      row1.payload.toVector shouldBe settings.durableStatePayloadCodec.nonePayload.toVector

      val ref2 = spawn(DurableStatePersister(persistenceId))
      ref2 ! DurableStatePersister.GetState(probe.ref)
      probe.expectMessage("") // after delete
      val msg = """{"a": "to be deleted"}""" // not important what we store
      ref2 ! DurableStatePersister.PersistWithAck(msg, probe.ref)
      probe.expectMessage(Done)

      val row2 = selectDurableStateRow(persistenceId)
      new String(row2.payload, UTF_8).clearWhitespace shouldBe msg.clearWhitespace

      // delete after some change
      ref2 ! DurableStatePersister.DeleteWithAck(probe.ref)
      probe.expectMessage(Done)
      testKit.stop(ref2)

      val ref3 = spawn(DurableStatePersister(persistenceId))
      ref3 ! DurableStatePersister.GetState(probe.ref)
      probe.expectMessage("") // after delete
      testKit.stop(ref3)

      val row3 = selectDurableStateRow(persistenceId)
      row3.payload.toVector shouldBe settings.durableStatePayloadCodec.nonePayload.toVector
    }
  }
}
