/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.r2dbc.{ ConnectionFactoryProvider, R2dbcSettings }
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.{ Serialization, SerializationExtension }
import com.typesafe.config.Config

import scala.concurrent.{ ExecutionContext, Future }

final class R2dbcSnapshotStore(cfg: Config, cfgPath: String) extends SnapshotStore {

  private implicit val ec: ExecutionContext = context.dispatcher
  private val serialization: Serialization = SerializationExtension(context.system)
  private implicit val system: ActorSystem[_] = context.system.toTyped

  private val dao = {
    val sharedConfigPath = cfgPath.replaceAll("""\.snapshot$""", "")
    val settings = new R2dbcSettings(context.system.settings.config.getConfig(sharedConfigPath))
    new SnapshotDao(
      settings,
      ConnectionFactoryProvider(system).connectionFactoryFor(sharedConfigPath + ".connection-factory"),
      serialization)
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    dao.load(persistenceId, criteria)

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    dao.store(metadata: SnapshotMetadata, snapshot: Any)

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val criteria =
      if (metadata.timestamp == 0L)
        SnapshotSelectionCriteria(maxSequenceNr = metadata.sequenceNr, minSequenceNr = metadata.sequenceNr)
      else
        SnapshotSelectionCriteria(
          maxSequenceNr = metadata.sequenceNr,
          minSequenceNr = metadata.sequenceNr,
          maxTimestamp = metadata.timestamp,
          minTimestamp = metadata.timestamp)
    deleteAsync(metadata.persistenceId, criteria)
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    dao.delete(persistenceId, criteria)
}
