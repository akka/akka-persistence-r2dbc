/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.AtomicWrite
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.SerializedEvent
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.SerializedEventMetadata
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import akka.persistence.journal.AsyncReplay
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object R2dbcJournal {
  case class WriteFinished(persistenceId: String, done: Future[_])

  def deserializeRow(serialization: Serialization, row: SerializedJournalRow): PersistentRepr = {
    if (row.payload.isEmpty)
      throw new IllegalStateException("Expected event payload to be loaded.")
    // note that FilteredPayload is not filtered out here, but that is handled by PersistentActor and EventSourcedBehavior
    val payload = serialization.deserialize(row.payload.get, row.serId, row.serManifest).get
    val repr = PersistentRepr(
      payload,
      row.seqNr,
      row.persistenceId,
      writerUuid = row.writerUuid,
      manifest = "", // FIXME issue #84
      deleted = false,
      sender = ActorRef.noSender)

    val reprWithMeta = row.metadata match {
      case None => repr
      case Some(meta) =>
        repr.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
    reprWithMeta
  }

  val FutureDone: Future[Done] = Future.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class R2dbcJournal(config: Config, cfgPath: String) extends AsyncWriteJournal with AsyncReplay {
  import R2dbcJournal.FutureDone
  import R2dbcJournal.WriteFinished
  import R2dbcJournal.deserializeRow

  implicit val system: ActorSystem[_] = context.system.toTyped
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[R2dbcJournal])

  private val persistenceExt = Persistence(system)

  private val sharedConfigPath = cfgPath.replaceAll("""\.journal$""", "")
  private val serialization: Serialization = SerializationExtension(context.system)
  private val settings = R2dbcSettings(context.system.settings.config.getConfig(sharedConfigPath))
  log.debug("R2DBC journal starting up with dialect [{}]", settings.dialectName)

  private val executorProvider =
    new R2dbcExecutorProvider(
      system,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, system),
      settings,
      sharedConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))
  private val journalDao =
    settings.connectionFactorySettings.dialect.createJournalDao(executorProvider)
  private val query = PersistenceQuery(system).readJournalFor[R2dbcReadJournal](sharedConfigPath + ".query")

  private val pubSub: Option[PubSub] =
    if (settings.journalPublishEvents) Some(PubSub(system))
    else None

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[_]]()

  override def receivePluginInternal: Receive = { case WriteFinished(pid, f) =>
    writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Instant] = {
      val timestamp = if (settings.useAppTimestamp) InstantFactory.now() else JournalDao.EmptyDbTimestamp
      val serialized: Try[Seq[SerializedJournalRow]] = Try {
        atomicWrite.payload.map { pr =>
          val (event, tags) = pr.payload match {
            case Tagged(payload, tags) =>
              (payload.asInstanceOf[AnyRef], tags)
            case other =>
              (other.asInstanceOf[AnyRef], Set.empty[String])
          }

          val entityType = PersistenceId.extractEntityType(pr.persistenceId)
          val slice = persistenceExt.sliceForPersistenceId(pr.persistenceId)

          val serializedEvent = event match {
            case s: SerializedEvent => s // already serialized
            case _ =>
              val bytes = serialization.serialize(event).get
              val serializer = serialization.findSerializerFor(event)
              val manifest = Serializers.manifestFor(serializer, event)
              new SerializedEvent(bytes, serializer.identifier, manifest)
          }

          val metadata = pr.metadata.map { meta =>
            val m = meta.asInstanceOf[AnyRef]
            val serializedMeta = serialization.serialize(m).get
            val metaSerializer = serialization.findSerializerFor(m)
            val metaManifest = Serializers.manifestFor(metaSerializer, m)
            val id: Int = metaSerializer.identifier
            SerializedEventMetadata(id, metaManifest, serializedMeta)
          }

          SerializedJournalRow(
            slice,
            entityType,
            pr.persistenceId,
            pr.sequenceNr,
            timestamp,
            JournalDao.EmptyDbTimestamp,
            Some(serializedEvent.bytes),
            serializedEvent.serializerId,
            serializedEvent.serializerManifest,
            pr.writerUuid,
            tags,
            metadata)
        }
      }

      serialized match {
        case Success(writes) =>
          journalDao.writeEvents(writes)
        case Failure(exc) =>
          Future.failed(exc)
      }
    }

    val persistenceId = messages.head.persistenceId
    val writeResult: Future[Instant] =
      if (messages.size == 1)
        atomicWrite(messages.head)
      else {
        // persistAsync case
        // easiest to just group all into a single AtomicWrite
        val batch = AtomicWrite(messages.flatMap(_.payload))
        atomicWrite(batch)
      }

    val writeAndPublishResult: Future[Done] =
      publish(messages, writeResult)

    writesInProgress.put(persistenceId, writeAndPublishResult)
    writeAndPublishResult.onComplete { _ =>
      self ! WriteFinished(persistenceId, writeAndPublishResult)
    }
    writeAndPublishResult.map(_ => Nil)(ExecutionContext.parasitic)
  }

  private def publish(messages: immutable.Seq[AtomicWrite], dbTimestamp: Future[Instant]): Future[Done] =
    pubSub match {
      case Some(ps) =>
        dbTimestamp.map { timestamp =>
          messages.iterator
            .flatMap(_.payload.iterator)
            .foreach(pr => ps.publish(pr, timestamp))

          Done
        }

      case None =>
        dbTimestamp.map(_ => Done)(ExecutionContext.parasitic)
    }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteEventsTo(persistenceId, toSequenceNr, resetSequenceNumber = false)
  }

  override def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Long] = {
    if (fromSequenceNr == -1)
      log.debug("replayMessages [{}] [{}]", persistenceId, "last")
    else
      log.debug("replayMessages [{}] [{}]", persistenceId, fromSequenceNr)

    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for [{}], deferring replayMessages until write completed", persistenceId)
        // we only want to make write - replay sequential, not fail if previous write failed
        f.recover { case _ => Done }(ExecutionContext.parasitic)
      case None => FutureDone
    }
    pendingWrite.flatMap { _ =>
      if (toSequenceNr <= 0 || max == 0) {
        // no replay
        journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr)
      } else if (fromSequenceNr == -1) {
        // recover from last event only
        query.internalLastEventByPersistenceId(persistenceId, toSequenceNr, includeDeleted = true).map {
          case Some(item) =>
            // payload is empty for deleted item
            if (item.payload.isDefined) {
              val repr = deserializeRow(serialization, item)
              recoveryCallback(repr)
            }
            item.seqNr
          case None =>
            0L
        }
      } else if (toSequenceNr == Long.MaxValue && max == Long.MaxValue) {
        // this is the normal case, highest sequence number from last event
        query
          .internalCurrentEventsByPersistenceId(
            persistenceId,
            fromSequenceNr,
            toSequenceNr,
            readHighestSequenceNr = false,
            includeDeleted = true)
          .runWith(Sink.fold(0L) { (_, item) =>
            // payload is empty for deleted item
            if (item.payload.isDefined) {
              val repr = deserializeRow(serialization, item)
              recoveryCallback(repr)
            }
            item.seqNr
          })
      } else {
        // replay to custom sequence number

        val highestSeqNr = journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr)

        val effectiveToSequenceNr =
          if (max == Long.MaxValue) toSequenceNr
          else math.min(toSequenceNr, fromSequenceNr + max - 1)

        query
          .internalCurrentEventsByPersistenceId(
            persistenceId,
            fromSequenceNr,
            effectiveToSequenceNr,
            readHighestSequenceNr = false,
            includeDeleted = false)
          .runWith(Sink
            .foreach { item =>
              val repr = deserializeRow(serialization, item)
              recoveryCallback(repr)
            })
          .flatMap(_ => highestSeqNr)
      }
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    throw new IllegalStateException(
      "asyncReplayMessages is not supposed to be called when implementing AsyncReplay. This is a bug, please report.")
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    throw new IllegalStateException(
      "asyncReplayMessages is not supposed to be called when implementing AsyncReplay. This is a bug, please report.")
  }
}
