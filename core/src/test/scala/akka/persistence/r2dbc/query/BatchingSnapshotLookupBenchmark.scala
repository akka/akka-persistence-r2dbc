/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink

/**
 * Manual benchmark validating, against real Postgres and a dataset shaped like the reported production workload - many
 * more distinct persistence ids in one partition than cache-capacity, so the snapshot-filtering stage sees a near-100%
 * cache-miss rate - that `batching.enabled` actually reduces wall-clock time for `eventsBySlicesStartingFromSnapshots`,
 * not just round-trip *count* in isolation.
 *
 * Each persistence id gets exactly one snapshot (seqNr 1) and one trailing event (seqNr 2). Since the event is always
 * after the snapshot, deciding it never needs to load the snapshot payload - only the sequence-number lookup - which
 * isolates the cost this change targets from every other cost in the pipeline.
 *
 * A plain `main`, not a test spec, so `sbt test` never runs it. Requires a running local Postgres matching
 * `docker/docker-compose-postgres.yml`. Run explicitly with: sbt "core/Test/runMain
 * akka.persistence.r2dbc.query.BatchingSnapshotLookupBenchmark" optionally followed by the number of persistence ids to
 * seed (default 8000).
 */
object BatchingSnapshotLookupBenchmark extends TestData {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val seedConcurrency = 200

  private val baselineConfig: Config =
    ConfigFactory
      .parseString("""
      akka.persistence.r2dbc.query.start-from-snapshot.enabled = true
      akka.persistence.r2dbc.query.start-from-snapshot.batching.enabled = false
      akka.persistence.r2dbc.journal.publish-events = off
      """)
      .withFallback(TestConfig.config)

  private val batchedConfig: Config =
    ConfigFactory
      .parseString("akka.persistence.r2dbc.query.start-from-snapshot.batching.enabled = true")
      .withFallback(baselineConfig)

  def main(args: Array[String]): Unit = {
    val numberOfPersistenceIds = args.headOption.map(_.toInt).getOrElse(8000)

    val baselineSystem =
      ActorSystem[Nothing](Behaviors.empty, "BatchingSnapshotLookupBenchmark-baseline", baselineConfig)
    implicit val ec: ExecutionContext = baselineSystem.executionContext

    val settings = R2dbcSettings(baselineConfig.getConfig("akka.persistence.r2dbc"))
    val executorProvider = new R2dbcExecutorProvider(
      baselineSystem,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, baselineSystem),
      settings,
      "akka.persistence.r2dbc.connection-factory",
      log)
    val persistenceExt = Persistence(baselineSystem)
    val dialect = settings.connectionFactorySettings.dialect
    val journalDao = dialect.createJournalDao(executorProvider)
    val snapshotDao = dialect.createSnapshotDao(executorProvider)

    log.info("Clearing journal/snapshot tables...")
    settings.allJournalTablesWithSchema.foreach { case (table, minSlice) =>
      Await.result(
        executorProvider.executorFor(minSlice).updateOne("delete")(_.createStatement(s"delete from $table")),
        10.seconds)
    }
    settings.allSnapshotTablesWithSchema.foreach { case (table, minSlice) =>
      Await.result(
        executorProvider.executorFor(minSlice).updateOne("delete")(_.createStatement(s"delete from $table")),
        10.seconds)
    }

    val entityType = nextEntityType()
    val stringSerializer = SerializationExtension(baselineSystem).serializerFor(classOf[String])
    val baseTime = Instant.now().minusSeconds(3600)

    def seedOne(i: Int): Future[Unit] = {
      val pid = nextPid(entityType)
      val slice = persistenceExt.sliceForPersistenceId(pid)
      val timestamp = baseTime.plusMillis(i.toLong)
      val snapshotRow = SerializedSnapshotRow(
        slice,
        entityType,
        pid,
        1L,
        timestamp,
        timestamp.toEpochMilli,
        stringSerializer.toBinary(s"snap-$pid"),
        stringSerializer.identifier,
        "",
        Set.empty,
        None)
      val eventRow = SerializedJournalRow(
        slice,
        entityType,
        pid,
        2L,
        timestamp,
        timestamp,
        Some(stringSerializer.toBinary(s"evt-$pid")),
        stringSerializer.identifier,
        "",
        "bench-writer",
        Set.empty,
        None)
      for {
        _ <- snapshotDao.store(snapshotRow)
        _ <- journalDao.writeEvents(Seq(eventRow))
      } yield ()
    }

    println(s"Seeding $numberOfPersistenceIds persistence ids (1 snapshot + 1 trailing event each)...")
    val seedStart = System.nanoTime()
    (1 to numberOfPersistenceIds).grouped(seedConcurrency).foreach { chunk =>
      Await.result(Future.traverse(chunk.toVector)(seedOne), 30.seconds)
    }
    println(s"Seeding done in ${(System.nanoTime() - seedStart) / 1000000} ms")

    def runQuery(sys: ActorSystem[_], label: String): FiniteDuration = {
      implicit val mat: akka.stream.Materializer = akka.stream.Materializer(sys)
      val query = PersistenceQuery(sys).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
      val start = System.nanoTime()
      val count = Await.result(
        query
          .currentEventsBySlicesStartingFromSnapshots[String, String](
            entityType,
            0,
            persistenceExt.numberOfSlices - 1,
            NoOffset,
            identity)
          .runWith(Sink.fold(0)((n, _) => n + 1)),
        5.minutes)
      val elapsed = (System.nanoTime() - start).nanos
      require(count == numberOfPersistenceIds, s"expected $numberOfPersistenceIds envelopes, got $count")
      val line = s"[$label] emitted $count envelopes in ${elapsed.toMillis} ms " +
        s"(${if (elapsed.toMillis == 0) "n/a" else (count * 1000L / elapsed.toMillis).toString} envelopes/sec)"
      println(line)
      elapsed
    }

    val baselineElapsed = runQuery(baselineSystem, "batching disabled (baseline)")

    val batchedSystem =
      ActorSystem[Nothing](Behaviors.empty, "BatchingSnapshotLookupBenchmark-batched", batchedConfig)
    val batchedElapsed =
      try runQuery(batchedSystem, "batching enabled")
      finally {
        batchedSystem.terminate()
        Await.result(batchedSystem.whenTerminated, 10.seconds)
      }

    val resultLine =
      f"RESULT: baseline ${baselineElapsed.toMillis} ms vs batched ${batchedElapsed.toMillis} ms " +
      f"(${baselineElapsed.toMillis.toDouble / math.max(1L, batchedElapsed.toMillis)}%.1fx)"
    println(resultLine)

    baselineSystem.terminate()
    Await.result(baselineSystem.whenTerminated, 10.seconds)
  }
}
