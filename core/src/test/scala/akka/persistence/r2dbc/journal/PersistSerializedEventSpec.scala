/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.SerializedEvent
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import org.scalatest.wordspec.AnyWordSpecLike

class PersistSerializedEventSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val settings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  case class Row(pid: String, seqNr: Long, serializerId: Int, manifest: String)

  "Persist SerializedEvent" should {

    "be stored with given serialization information" in {
      val doneProbe = createTestProbe[Done]()

      val entityType = nextEntityType()
      val persistenceId = PersistenceId.ofUniqueId(nextPid(entityType))
      val slice = persistenceExt.sliceForPersistenceId(persistenceId.id)
      val ref = spawn(Persister(persistenceId, Set.empty))

      // String serialization has no manifest
      val event1 = "e1"
      val serializer1 = SerializationExtension(system).findSerializerFor(event1)
      val serializedEvent1 =
        new SerializedEvent(
          serializer1.toBinary(event1),
          serializer1.identifier,
          Serializers.manifestFor(serializer1, event1))

      // Option serialization has manifest
      val event2 = Some("e2")
      val serializer2 = SerializationExtension(system).findSerializerFor(event2)
      val serializedEvent2 =
        new SerializedEvent(
          serializer2.toBinary(event2),
          serializer2.identifier,
          Serializers.manifestFor(serializer2, event2))

      ref ! Persister.Persist(serializedEvent1)
      ref ! Persister.PersistWithAck(serializedEvent2, doneProbe.ref)
      doneProbe.expectMessage(Done)
      testKit.stop(ref)

      val ref2 = spawn(Persister(persistenceId, Set.empty))
      val replyProbe = createTestProbe[String]()
      ref2 ! Persister.GetState(replyProbe.ref)
      replyProbe.expectMessage("e1|Some(e2)")

      val rows =
        r2dbcExecutor
          .select[Row]("test")(
            connection =>
              connection.createStatement(
                s"select * from ${settings.journalTableWithSchema(slice)} where persistence_id = '${persistenceId.id}'"),
            row => {
              Row(
                pid = row.get("persistence_id", classOf[String]),
                seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
                serializerId = row.get[java.lang.Integer]("event_ser_id", classOf[java.lang.Integer]),
                manifest = row.get("event_ser_manifest", classOf[String]))
            })
          .futureValue

      rows.foreach { case Row(pid, seqNr, serializerId, manifest) =>
        withClue(s"pid [$pid}], seqNr [$seqNr]: ") {
          if (seqNr == 1L) {
            serializerId shouldBe serializedEvent1.serializerId
            manifest shouldBe serializedEvent1.serializerManifest
          } else if (seqNr == 2L) {
            serializerId shouldBe serializedEvent2.serializerId
            manifest shouldBe serializedEvent2.serializerManifest
          }
        }
      }
    }

  }
}
