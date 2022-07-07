/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
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
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.AtomicWrite
import akka.persistence.Persistence
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.journal.JournalDao.SerializedEventMetadata
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object R2dbcJournal {
  case class WriteFinished(persistenceId: String, done: Future[_])

  def deserializeRow(serialization: Serialization, row: SerializedJournalRow): PersistentRepr = {
    if (row.payload.isEmpty)
      throw new IllegalStateException("Expected event payload to be loaded.")
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
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class R2dbcJournal(config: Config, cfgPath: String) extends AsyncWriteJournal {
  import R2dbcJournal.WriteFinished
  import R2dbcJournal.deserializeRow

  implicit val system: ActorSystem[_] = context.system.toTyped
  implicit val ec: ExecutionContext = context.dispatcher

  private val log = Logging(context.system, classOf[R2dbcJournal])

  private val persistenceExt = Persistence(system)

  private val sharedConfigPath = cfgPath.replaceAll("""\.journal$""", "")
  private val serialization: Serialization = SerializationExtension(context.system)
  private val journalSettings = R2dbcSettings(context.system.settings.config.getConfig(sharedConfigPath))

  private val journalDao =
    new JournalDao(
      journalSettings,
      ConnectionFactoryProvider(system).connectionFactoryFor(sharedConfigPath + ".connection-factory"))
  private val query = PersistenceQuery(system).readJournalFor[R2dbcReadJournal](sharedConfigPath + ".query")

  private val pubSub: Option[PubSub] =
    if (journalSettings.journalPublishEvents) Some(PubSub(system))
    else None

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[_]]()

  private var eventsByTagNotImplementedLogged = false

  override def receivePluginInternal: Receive = { case WriteFinished(pid, f) =>
    writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    def atomicWrite(atomicWrite: AtomicWrite): Future[Instant] = {
      val timestamp = if (journalSettings.useAppTimestamp) Instant.now() else JournalDao.EmptyDbTimestamp
      val serialized: Try[Seq[SerializedJournalRow]] = Try {
        atomicWrite.payload.map { pr =>
          val (event, tags) = pr.payload match {
            case Tagged(payload, tags) =>
              // eventsBytag not implemented, issue #82, but they are stored
              logEventsByTagsNotImplemented()
              (payload.asInstanceOf[AnyRef], tags)
            case other =>
              (other.asInstanceOf[AnyRef], Set.empty[String])
          }

          val entityType = PersistenceId.extractEntityType(pr.persistenceId)
          val slice = persistenceExt.sliceForPersistenceId(pr.persistenceId)

          val serialized = serialization.serialize(event).get
          val serializer = serialization.findSerializerFor(event)
          val manifest = Serializers.manifestFor(serializer, event)
          val id: Int = serializer.identifier

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
            Some(serialized),
            id,
            manifest,
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
    writeAndPublishResult.map(_ => Nil)(ExecutionContexts.parasitic)
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
        dbTimestamp.map(_ => Done)(ExecutionContexts.parasitic)
    }

  private def logEventsByTagsNotImplemented(): Unit = {
    if (!eventsByTagNotImplementedLogged) {
      eventsByTagNotImplementedLogged = true
      log.info(
        "eventsByTag query not implemented by akka-persistence-r2dbc. We recommend using eventsBySlices instead. " +
        "The given tags are stored. " +
        "eventsByTag may be implemented in the future if there is strong demand for it. " +
        "Let us know in https://github.com/akka/akka-persistence-r2dbc/issues/82")
    }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteMessagesTo(persistenceId, toSequenceNr)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug("asyncReplayMessages persistenceId [{}], fromSequenceNr [{}]", persistenceId, fromSequenceNr)
    val effectiveToSequenceNr =
      if (max == Long.MaxValue) toSequenceNr
      else math.min(toSequenceNr, fromSequenceNr + max - 1)
    query
      .internalEventsByPersistenceId(persistenceId, fromSequenceNr, effectiveToSequenceNr)
      .runWith(Sink.foreach { row =>
        val repr = deserializeRow(serialization, row)
        recoveryCallback(repr)
      })
      .map(_ => ())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr [{}] [{}]", persistenceId, fromSequenceNr)
    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for [{}], deferring highest seq nr until write completed", persistenceId)
        // we only want to make write - replay sequential, not fail if previous write failed
        f.recover { case _ => Done }(ExecutionContexts.parasitic)
      case None => Future.successful(Done)
    }
    pendingWrite.flatMap(_ => journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr))
  }
}
