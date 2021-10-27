/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.r2dbc.state.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao.SerializedStateRow
import akka.persistence.state.scaladsl.DurableStateUpdateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object R2dbcDurableStateStore {
  val Identifier = "akka.persistence.r2dbc.state"
}

class R2dbcDurableStateStore[A](system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateUpdateStore[A] {

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.state$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))

  private val typedSystem = system.toTyped
  import typedSystem.executionContext
  private val serialization = SerializationExtension(system)
  private val stateDao =
    new DurableStateDao(
      settings,
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(sharedConfigPath + ".connection-factory"))(
      typedSystem.executionContext,
      typedSystem)

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    stateDao.readState(persistenceId).map {
      case None => GetObjectResult(None, 0L)
      case Some(serializedRow) =>
        val payload = serialization
          .deserialize(serializedRow.payload, serializedRow.serId, serializedRow.serManifest)
          .get
          .asInstanceOf[A]
        GetObjectResult(Some(payload), serializedRow.revision)
    }
  }

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] = {
    val valueAnyRef = value.asInstanceOf[AnyRef]
    val serialized = serialization.serialize(valueAnyRef).get
    val serializer = serialization.findSerializerFor(valueAnyRef)
    val manifest = Serializers.manifestFor(serializer, valueAnyRef)
    val timestamp = System.currentTimeMillis()

    val serializedRow = SerializedStateRow(
      persistenceId,
      revision,
      DurableStateDao.EmptyDbTimestamp,
      DurableStateDao.EmptyDbTimestamp,
      timestamp,
      serialized,
      serializer.identifier,
      manifest)

    stateDao.writeState(serializedRow)

  }

  override def deleteObject(persistenceId: String): Future[Done] =
    stateDao.deleteState(persistenceId)

}
