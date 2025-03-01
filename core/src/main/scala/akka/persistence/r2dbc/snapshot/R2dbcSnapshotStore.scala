/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import java.time.Instant

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria }
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.{ Serialization, SerializationExtension }
import com.typesafe.config.Config
import scala.concurrent.{ ExecutionContext, Future }

import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotMetadata
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.typed.PersistenceId
import akka.serialization.Serializers

object R2dbcSnapshotStore {
  private def deserializeSnapshotRow(snap: SerializedSnapshotRow, serialization: Serialization): SelectedSnapshot =
    SelectedSnapshot(
      SnapshotMetadata(
        snap.persistenceId,
        snap.seqNr,
        snap.writeTimestamp,
        snap.metadata.map(serializedMeta =>
          serialization
            .deserialize(serializedMeta.payload, serializedMeta.serializerId, serializedMeta.serializerManifest)
            .get)),
      serialization.deserialize(snap.snapshot, snap.serializerId, snap.serializerManifest).get)

  private val FutureNone = Future.successful(None)
}

/**
 * INTERNAL API
 *
 * Note: differs from other snapshot stores in that in does not retain old snapshots but keeps a single snapshot per
 * entity that is updated.
 */
@InternalApi
private[r2dbc] final class R2dbcSnapshotStore(cfg: Config, cfgPath: String) extends SnapshotStore {
  import R2dbcSnapshotStore.deserializeSnapshotRow
  import R2dbcSnapshotStore.FutureNone

  private implicit val ec: ExecutionContext = context.dispatcher
  private val serialization: Serialization = SerializationExtension(context.system)
  private implicit val system: ActorSystem[_] = context.system.toTyped
  private val persistenceExt = Persistence(system)

  val sharedConfigPath = cfgPath.replaceAll("""\.snapshot$""", "")
  val settings = R2dbcSettings(context.system.settings.config.getConfig(sharedConfigPath))
  log.debug("R2DBC snapshot store starting up with dialect [{}]", settings.dialectName)

  private val executorProvider =
    new R2dbcExecutorProvider(
      system,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, system),
      settings,
      sharedConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))
  private val dao = settings.connectionFactorySettings.dialect.createSnapshotDao(executorProvider)
  private val queryDao = settings.connectionFactorySettings.dialect.createQueryDao(executorProvider)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    if (criteria.maxSequenceNr <= 0)
      FutureNone
    else
      dao
        .load(persistenceId, criteria)
        .map(_.map(row => deserializeSnapshotRow(row, serialization)))
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(metadata.persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(metadata.persistenceId)

    val snapshotAnyRef = snapshot.asInstanceOf[AnyRef]
    val serializedSnapshot = serialization.serialize(snapshotAnyRef).get
    val snapshotSerializer = serialization.findSerializerFor(snapshotAnyRef)
    val snapshotManifest = Serializers.manifestFor(snapshotSerializer, snapshotAnyRef)

    val serializedMeta: Option[SerializedSnapshotMetadata] = metadata.metadata.map { meta =>
      val metaRef = meta.asInstanceOf[AnyRef]
      val serializedMeta = serialization.serialize(metaRef).get
      val metaSerializer = serialization.findSerializerFor(metaRef)
      val metaManifest = Serializers.manifestFor(metaSerializer, metaRef)
      SerializedSnapshotMetadata(serializedMeta, metaSerializer.identifier, metaManifest)
    }

    val correspondingEvent: Future[Option[JournalDao.SerializedJournalRow]] =
      if (settings.querySettings.startFromSnapshotEnabled)
        queryDao.loadEvent(metadata.persistenceId, metadata.sequenceNr, includePayload = false)
      else
        Future.successful(None)

    // use same timestamp and tags as the corresponding event, if startFromSnapshotEnabled and event exists
    correspondingEvent.flatMap { eventEnvOpt =>
      val (timestamp, tags) = eventEnvOpt match {
        case Some(eventEnv) =>
          (eventEnv.dbTimestamp, eventEnv.tags)
        case None =>
          (Instant.ofEpochMilli(metadata.timestamp), Set.empty[String])
      }

      val serializedRow = SerializedSnapshotRow(
        slice,
        entityType,
        metadata.persistenceId,
        metadata.sequenceNr,
        timestamp,
        metadata.timestamp,
        serializedSnapshot,
        snapshotSerializer.identifier,
        snapshotManifest,
        tags,
        serializedMeta)

      dao.store(serializedRow)
    }
  }

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
