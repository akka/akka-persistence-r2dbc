/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.dispatch.ExecutionContexts
import akka.pattern.ask
import akka.persistence.Persistence
import akka.persistence.SelectedSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence.SnapshotProtocol.LoadSnapshotResult
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.SerializedEventMetadata
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.migration.MigrationToolDao.CurrentProgress
import SnapshotDao.SerializedSnapshotMetadata
import SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import org.slf4j.LoggerFactory

import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.stream.scaladsl.Source

object MigrationTool {
  def main(args: Array[String]): Unit = {
    ActorSystem(MigrationTool(), "MigrationTool")
  }

  object Result {
    val empty: Result = Result(0, 0, 0)
  }
  final case class Result(persistenceIds: Long, events: Long, snapshots: Long)

  object DurableStateResult {
    val empty: DurableStateResult = DurableStateResult(0, 0)
  }
  final case class DurableStateResult(persistenceIds: Long, states: Long)

  private def apply(): Behavior[Try[Result]] = {
    Behaviors.setup { context =>
      val migration = new MigrationTool(context.system)
      context.pipeToSelf(migration.migrateAll()) { result =>
        result
      }

      Behaviors.receiveMessage {
        case Success(_) =>
          // result already logged by migrateAll
          Behaviors.stopped
        case Failure(_) =>
          Behaviors.stopped
      }
    }
  }

  private final case class SelectedDurableState(persistenceId: String, revision: Long, value: Any)

}

/**
 * Migration from another Akka Persistence plugin to the R2DBC plugin. Converts events and snapshots. It has been tested
 * with `akka-persistence-jdbc` as source plugin, but it should work with any plugin that has support for
 * `CurrentPersistenceIdsQuery` and `CurrentEventsByPersistenceIdQuery`.
 *
 * It can be run as a main class `akka.persistence.r2dbc.migration.MigrationTool` with configuration in
 * `application.conf` or embedded in an application by creating an instance of `MigrationTool` and invoking
 * `migrateAll`.
 *
 * It can be run while the source system is still active and it can be run multiple times with idempotent result. To
 * speed up processing of subsequent runs it stores migrated persistence ids and sequence numbers in the table
 * `migration_progress`. In a subsequent run it will only migrate new events and snapshots compared to what was stored
 * in `migration_progress`. It will also find and migrate new persistence ids in a subsequent run. You can delete from
 * `migration_progress` if you want to re-run the full migration.
 *
 * Note: tags are not migrated.
 */
class MigrationTool(system: ActorSystem[_]) {
  import MigrationTool.{ DurableStateResult, Result }
  import MigrationTool.SelectedDurableState
  import system.executionContext
  private implicit val sys: ActorSystem[_] = system

  private val log = LoggerFactory.getLogger(getClass)

  private val persistenceExt = Persistence(system)

  private val migrationConfig = system.settings.config.getConfig("akka.persistence.r2dbc.migration")

  private val parallelism = migrationConfig.getInt("parallelism")

  private val targetPluginId = migrationConfig.getString("target.persistence-plugin-id")
  private val targetR2dbcSettings = R2dbcSettings(system.settings.config.getConfig(targetPluginId))

  private val serialization: Serialization = SerializationExtension(system)

  private val targetExecutorProvider = new R2dbcExecutorProvider(
    targetR2dbcSettings,
    targetPluginId + ".connection-factory",
    LoggerFactory.getLogger(getClass))

  private val targetJournalDao =
    targetR2dbcSettings.connectionFactorySettings.dialect.createJournalDao(targetR2dbcSettings, targetExecutorProvider)
  private val targetSnapshotDao =
    targetR2dbcSettings.connectionFactorySettings.dialect
      .createSnapshotDao(targetR2dbcSettings, targetExecutorProvider)
  private val targetDurableStateDao =
    targetR2dbcSettings.connectionFactorySettings.dialect
      .createDurableStateDao(targetR2dbcSettings, targetExecutorProvider)

  private val targetBatch = migrationConfig.getInt("target.batch")

  private val sourceQueryPluginId = migrationConfig.getString("source.query-plugin-id")
  private val sourceReadJournal = PersistenceQuery(system).readJournalFor[ReadJournal](sourceQueryPluginId)
  private val sourcePersistenceIdsQuery = sourceReadJournal.asInstanceOf[CurrentPersistenceIdsQuery]
  private val sourceEventsByPersistenceIdQuery = sourceReadJournal.asInstanceOf[CurrentEventsByPersistenceIdQuery]

  private val sourceSnapshotPluginId = migrationConfig.getString("source.snapshot-plugin-id")
  private lazy val sourceSnapshotStore = Persistence(system).snapshotStoreFor(sourceSnapshotPluginId)

  private val sourceDurableStatePluginId = migrationConfig.getString("source.durable-state-plugin-id")
  private lazy val sourceDurableStateStore =
    DurableStateStoreRegistry(system).durableStateStoreFor[DurableStateStore[Any]](sourceDurableStatePluginId)

  if (targetR2dbcSettings.dialectName == "h2") {
    log.error("Migrating to H2 using the migration tool not currently supported")
  }
  private[r2dbc] val migrationDao = new MigrationToolDao(targetExecutorProvider)

  private lazy val createProgressTable: Future[Done] =
    migrationDao.createProgressTable()

  /**
   * Migrates events, snapshots for all persistence ids.
   *
   * Note that Durable State is not migrated by this method, instead you need to use
   * [[MigrationTool#migrateDurableStates]] for a given list of persistence ids.
   */
  def migrateAll(): Future[Result] = {
    log.info("Migration started.")
    val result =
      sourcePersistenceIdsQuery
        .currentPersistenceIds()
        .mapAsyncUnordered(parallelism) { persistenceId =>
          for {
            _ <- createProgressTable
            currentProgress <- migrationDao.currentProgress(persistenceId)
            eventCount <- migrateEvents(persistenceId, currentProgress)
            snapshotCount <- migrateSnapshot(persistenceId, currentProgress)
          } yield persistenceId -> Result(1, eventCount, snapshotCount)
        }
        .map { case (pid, result @ Result(_, events, snapshots)) =>
          log.debugN(
            "Migrated persistenceId [{}] with [{}] events{}.",
            pid,
            events,
            if (snapshots == 0) "" else " and snapshot")
          result
        }
        .runWith(Sink.fold(Result.empty) { case (acc, Result(_, events, snapshots)) =>
          val result = Result(acc.persistenceIds + 1, acc.events + events, acc.snapshots + snapshots)
          if (result.persistenceIds % 100 == 0)
            log.infoN(
              "Migrated [{}] persistenceIds with [{}] events and [{}] snapshots.",
              result.persistenceIds,
              result.events,
              result.snapshots)
          result
        })

    result.transform {
      case s @ Success(Result(persistenceIds, events, snapshots)) =>
        log.infoN(
          "Migration successful. Migrated [{}] persistenceIds with [{}] events and [{}] snapshots.",
          persistenceIds,
          events,
          snapshots)
        s
      case f @ Failure(exc) =>
        log.error("Migration failed.", exc)
        f
    }
  }

  /**
   * Migrate events for a single persistence id.
   */
  def migrateEvents(persistenceId: String): Future[Long] = {
    for {
      _ <- createProgressTable
      currentProgress <- migrationDao.currentProgress(persistenceId)
      eventCount <- migrateEvents(persistenceId, currentProgress)
    } yield eventCount
  }

  private def migrateEvents(persistenceId: String, currentProgress: Option[CurrentProgress]): Future[Long] = {
    val progressSeqNr = currentProgress.map(_.eventSeqNr).getOrElse(0L)
    sourceEventsByPersistenceIdQuery
      .currentEventsByPersistenceId(persistenceId, progressSeqNr + 1, Long.MaxValue)
      .map(serializedJournalRow)
      .grouped(targetBatch)
      .mapAsync(1) { events =>
        targetJournalDao
          .writeEvents(events)
          .recoverWith { case _: R2dbcDataIntegrityViolationException =>
            // events already exists, which is ok, but since the batch
            // failed we must try again one-by-one
            Future.sequence(events.map { event =>
              targetJournalDao
                .writeEvents(List(event))
                .recoverWith { case _: R2dbcDataIntegrityViolationException =>
                  // ok, already exists
                  log
                    .debug("event already exists, persistenceId [{}], seqNr [{}]", event.persistenceId, event.seqNr)
                  Future.successful(())
                }
            })
          }
          .map(_ => events.last.seqNr -> events.size)
      }
      .mapAsync(1) { case (seqNr, count) =>
        migrationDao
          .updateEventProgress(persistenceId, seqNr)
          .map(_ => count)
      }
      .runWith(Sink.fold(0L) { case (acc, count) => acc + count })
  }

  private def serializedJournalRow(env: ClassicEventEnvelope): SerializedJournalRow = {
    val entityType = PersistenceId.extractEntityType(env.persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(env.persistenceId)

    val event = env.event.asInstanceOf[AnyRef]
    val serialized = serialization.serialize(event).get
    val serializer = serialization.findSerializerFor(event)
    val manifest = Serializers.manifestFor(serializer, event)

    val metadata =
      env.eventMetadata.map { meta =>
        val m = meta.asInstanceOf[AnyRef]
        val serializedMeta = serialization.serialize(m).get
        val metaSerializer = serialization.findSerializerFor(m)
        val metaManifest = Serializers.manifestFor(metaSerializer, m)
        SerializedEventMetadata(metaSerializer.identifier, metaManifest, serializedMeta)
      }

    SerializedJournalRow(
      slice,
      entityType,
      env.persistenceId,
      env.sequenceNr,
      Instant.ofEpochMilli(env.timestamp),
      JournalDao.EmptyDbTimestamp,
      Some(serialized),
      serializer.identifier,
      manifest,
      "", // writerUuid is discarded, but that is ok
      tags = Set.empty, // tags are not migrated (not included in currentEventsByPersistenceId envelope)
      metadata)
  }

  /**
   * Migrate latest snapshot for a single persistence id.
   */
  def migrateSnapshot(persistenceId: String): Future[Int] = {
    for {
      _ <- createProgressTable
      currentProgress <- migrationDao.currentProgress(persistenceId)
      snapCount <- migrateSnapshot(persistenceId, currentProgress)
    } yield snapCount
  }

  private def migrateSnapshot(persistenceId: String, currentProgress: Option[CurrentProgress]): Future[Int] = {
    val progressSeqNr = currentProgress.map(_.snapshotSeqNr).getOrElse(0L)
    loadSourceSnapshot(persistenceId, progressSeqNr + 1).flatMap {
      case None => Future.successful(0)
      case Some(selectedSnapshot @ SelectedSnapshot(snapshotMetadata, _)) =>
        for {
          seqNr <- {
            // We could load the timestamp and tags from corresponding event, see R2dbcSnapshotStore.saveAsync,
            // but when enabling eventsBySlicesStartingFromSnapshots the sql updates should anyway be run.
            // See https://doc.akka.io/docs/akka-persistence-r2dbc/current/migration-guide.html#eventsBySlicesStartingFromSnapshots
            val serializedRow = serializedSnapotRow(selectedSnapshot)
            targetSnapshotDao
              .store(serializedRow)
              .map(_ => snapshotMetadata.sequenceNr)(ExecutionContexts.parasitic)
          }
          _ <- migrationDao.updateSnapshotProgress(persistenceId, seqNr)
        } yield 1
    }
  }

  private def serializedSnapotRow(selectedSnapshot: SelectedSnapshot): SerializedSnapshotRow = {
    val snapshotMetadata = selectedSnapshot.metadata
    val snapshotAnyRef = selectedSnapshot.snapshot.asInstanceOf[AnyRef]
    val serializedSnapshot = serialization.serialize(snapshotAnyRef).get
    val snapshotSerializer = serialization.findSerializerFor(snapshotAnyRef)
    val snapshotManifest = Serializers.manifestFor(snapshotSerializer, snapshotAnyRef)

    val serializedMeta: Option[SerializedSnapshotMetadata] = snapshotMetadata.metadata.map { meta =>
      val metaRef = meta.asInstanceOf[AnyRef]
      val serializedMeta = serialization.serialize(metaRef).get
      val metaSerializer = serialization.findSerializerFor(metaRef)
      val metaManifest = Serializers.manifestFor(metaSerializer, metaRef)
      SerializedSnapshotMetadata(serializedMeta, metaSerializer.identifier, metaManifest)
    }

    val slice = persistenceExt.sliceForPersistenceId(snapshotMetadata.persistenceId)
    val entityType = PersistenceId.extractEntityType(snapshotMetadata.persistenceId)

    val serializedRow = SerializedSnapshotRow(
      slice,
      entityType,
      snapshotMetadata.persistenceId,
      snapshotMetadata.sequenceNr,
      Instant.ofEpochMilli(snapshotMetadata.timestamp),
      snapshotMetadata.timestamp,
      serializedSnapshot,
      snapshotSerializer.identifier,
      snapshotManifest,
      tags = Set.empty,
      serializedMeta)
    serializedRow
  }

  private def loadSourceSnapshot(persistenceId: String, minSequenceNr: Long): Future[Option[SelectedSnapshot]] = {
    if (sourceSnapshotPluginId == "")
      Future.successful(None)
    else {
      implicit val timeout: Timeout = 10.seconds
      val criteria = SnapshotSelectionCriteria.Latest
      (sourceSnapshotStore ? LoadSnapshot(persistenceId, criteria, Long.MaxValue))
        .mapTo[LoadSnapshotResult]
        .map(result => result.snapshot.flatMap(s => if (s.metadata.sequenceNr >= minSequenceNr) Some(s) else None))
    }

  }

  /**
   * Migrate Durable State for a list of persistence ids.
   */
  def migrateDurableStates(persistenceIds: Seq[String]): Future[DurableStateResult] = {
    log.info("Migration started.")
    val result =
      Source(persistenceIds)
        .mapAsyncUnordered(parallelism) { persistenceId =>
          for {
            _ <- createProgressTable
            currentProgress <- migrationDao.currentProgress(persistenceId)
            stateCount <- migrateDurableState(persistenceId, currentProgress)
          } yield persistenceId -> DurableStateResult(1, stateCount)
        }
        .map { case (pid, result @ DurableStateResult(_, states)) =>
          log.debugN("Migrated persistenceId [{}] with [{}] durable state.", pid, states)
          result
        }
        .runWith(Sink.fold(DurableStateResult.empty) { case (acc, DurableStateResult(_, states)) =>
          val result = DurableStateResult(acc.persistenceIds + 1, acc.states + states)
          if (result.persistenceIds % 100 == 0)
            log.infoN("Migrated [{}] persistenceIds with [{}] durable states.", result.persistenceIds, result.states)
          result
        })

    result.transform {
      case s @ Success(DurableStateResult(persistenceIds, states)) =>
        log.infoN(
          "Migration successful. Migrated [{}] persistenceIds with [{}] durable states.",
          persistenceIds,
          states)
        s
      case f @ Failure(exc) =>
        log.error("Migration failed.", exc)
        f
    }
  }

  /**
   * Migrate Durable State for a single persistence id.
   */
  def migrateDurableState(persistenceId: String): Future[Int] = {
    for {
      _ <- createProgressTable
      currentProgress <- migrationDao.currentProgress(persistenceId)
      stateCount <- migrateDurableState(persistenceId, currentProgress)
    } yield stateCount
  }

  private def migrateDurableState(persistenceId: String, currentProgress: Option[CurrentProgress]): Future[Int] = {
    val progressRevision = currentProgress.map(_.durableStateRevision).getOrElse(0L)
    loadSourceDurableState(persistenceId, progressRevision + 1).flatMap {
      case None => Future.successful(0)
      case Some(selectedDurableState) =>
        for {
          revision <- {
            val serializedRow = serializedDurableStateRow(selectedDurableState)
            targetDurableStateDao
              .upsertState(serializedRow, selectedDurableState.value, changeEvent = None)
              .map(_ => selectedDurableState.revision)(ExecutionContexts.parasitic)
          }
          _ <- migrationDao.updateDurableStateProgress(persistenceId, revision)
        } yield 1
    }
  }

  private lazy val checkAssertSingleWriter: Unit = {
    if (targetR2dbcSettings.durableStateAssertSingleWriter) {
      throw new IllegalArgumentException(
        "When running the MigrationTool the " +
        "`akka.persistence.r2dbc.state.assert-single-writer` configuration must be set to off.")
    }
  }

  private def serializedDurableStateRow(selectedDurableState: SelectedDurableState): SerializedStateRow = {
    val stateAnyRef = selectedDurableState.value.asInstanceOf[AnyRef]
    val serializedState = serialization.serialize(stateAnyRef).get
    val stateSerializer = serialization.findSerializerFor(stateAnyRef)
    val stateManifest = Serializers.manifestFor(stateSerializer, stateAnyRef)

    // not possible to preserve timestamp, because not included in GetObjectResult
    val timestamp = Instant.now()

    val serializedRow = SerializedStateRow(
      selectedDurableState.persistenceId,
      selectedDurableState.revision,
      timestamp,
      timestamp,
      Some(serializedState),
      stateSerializer.identifier,
      stateManifest,
      tags = Set.empty // not possible to preserve tags, because not included in GetObjectResult
    )
    serializedRow
  }

  private def loadSourceDurableState(persistenceId: String, minRevision: Long): Future[Option[SelectedDurableState]] = {
    sourceDurableStateStore
      .getObject(persistenceId)
      .map {
        case GetObjectResult(Some(value), revision) if revision >= minRevision =>
          Some(SelectedDurableState(persistenceId, revision, value))
        case _ => None
      }
  }

}
