/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpecLike

class PersistenceIdsBySlicesSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query =
    PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private val dao = settings.connectionFactorySettings.dialect.createQueryDao(r2dbcExecutorProvider)

  private def canBeTestedWithDialect: Boolean =
    settings.dialectName == "postgres" || settings.dialectName == "h2"

  private def pendingIfCannotBeTestedWithDialect(): Unit = {
    if (!canBeTestedWithDialect) {
      info(s"Can't be tested with dialect [${settings.dialectName}]")
      pending
    }
  }

  "persistenceIdsBySlices" should {

    pendingIfMoreThanOneDataPartition()
    pendingIfCannotBeTestedWithDialect()

    "return all active persistence ids when no offset is given" in {
      val entityType = nextEntityType()
      val pids = (1 to 5).map(_ => nextPid(entityType)).toVector
      val now = InstantFactory.now()
      pids.foreach { pid =>
        val slice = persistenceExt.sliceForPersistenceId(pid)
        writeEvent(slice, pid, 1L, now, "e-1")
      }

      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result.toSet shouldBe pids.toSet
    }

    "filter out persistence ids whose latest event is older than the offset timestamp" in {

      val entityType = nextEntityType()
      val oldPids = (1 to 3).map(_ => nextPid(entityType)).toVector
      val newPids = (1 to 2).map(_ => nextPid(entityType)).toVector
      val now = InstantFactory.now()
      val oldTime = now.minusSeconds(3600)
      val newTime = now.minusSeconds(60)

      oldPids.foreach { pid =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, oldTime, "old")
      }
      newPids.foreach { pid =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, newTime, "new")
      }

      val cutoff = now.minusSeconds(600)
      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = TimestampOffset(cutoff, Map.empty),
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result.toSet shouldBe newPids.toSet
    }

    "include a persistence id whose latest event is at or after the offset, even if it has older events" in {

      val entityType = nextEntityType()
      val pidWithRecentEvent = nextPid(entityType)
      val pidOnlyOld = nextPid(entityType)
      val now = InstantFactory.now()
      val oldTime = now.minusSeconds(3600)
      val recentTime = now.minusSeconds(60)

      val slice1 = persistenceExt.sliceForPersistenceId(pidWithRecentEvent)
      writeEvent(slice1, pidWithRecentEvent, 1L, oldTime, "old")
      writeEvent(slice1, pidWithRecentEvent, 2L, recentTime, "recent")
      writeEvent(persistenceExt.sliceForPersistenceId(pidOnlyOld), pidOnlyOld, 1L, oldTime, "old")

      val cutoff = now.minusSeconds(600)
      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = TimestampOffset(cutoff, Map.empty),
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe Vector(pidWithRecentEvent)
    }

    "respect the limit" in {

      val entityType = nextEntityType()
      val pids = (1 to 10).map(_ => nextPid(entityType)).toVector
      val now = InstantFactory.now()
      pids.foreach { pid =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, now, "e-1")
      }

      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 3)
        .runWith(Sink.seq)
        .futureValue

      result.size shouldBe 3
      result.toSet.subsetOf(pids.toSet) shouldBe true
    }

    "filter by slice range" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      // Write pids into half slices each so the filter is exercised regardless of pid hashing.
      val midSlice = persistenceExt.numberOfSlices / 2
      val lowPids = (1 to 3).map(_ => nextPid(entityType)).toVector
      val highPids = (1 to 3).map(_ => nextPid(entityType)).toVector
      lowPids.zipWithIndex.foreach { case (pid, i) => writeEvent(i, pid, 1L, now, "lo") }
      highPids.zipWithIndex.foreach { case (pid, i) => writeEvent(midSlice + i, pid, 1L, now, "hi") }

      val lowResult = query
        .persistenceIdsBySlices(entityType, 0, midSlice - 1, NoOffset, NoOffset, 100)
        .runWith(Sink.seq)
        .futureValue
      lowResult.toSet shouldBe lowPids.toSet

      val highResult = query
        .persistenceIdsBySlices(entityType, midSlice, persistenceExt.numberOfSlices - 1, NoOffset, NoOffset, 100)
        .runWith(Sink.seq)
        .futureValue
      highResult.toSet shouldBe highPids.toSet
    }

    "filter by entity type" in {

      val entityTypeA = nextEntityType()
      val entityTypeB = nextEntityType()
      val pidsA = (1 to 3).map(_ => nextPid(entityTypeA)).toVector
      val pidsB = (1 to 3).map(_ => nextPid(entityTypeB)).toVector
      val now = InstantFactory.now()
      pidsA.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, now, "a"))
      pidsB.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, now, "b"))

      val resultA = query
        .persistenceIdsBySlices(
          entityTypeA,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      resultA.toSet shouldBe pidsA.toSet
    }

    "order results by most recently active persistence id first" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      // pids written from oldest to newest by index
      val pids = (1 to 5).map(_ => nextPid(entityType)).toVector
      pids.zipWithIndex.foreach { case (pid, i) =>
        val timestamp = now.minusSeconds((5 - i) * 60L)
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, timestamp, "e-1")
      }

      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe pids.reverse
    }

    "return the most recently active ids when limit is smaller than total, in recency order" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val pids = (1 to 5).map(_ => nextPid(entityType)).toVector
      pids.zipWithIndex.foreach { case (pid, i) =>
        val timestamp = now.minusSeconds((5 - i) * 60L)
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, timestamp, "e-1")
      }

      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 3)
        .runWith(Sink.seq)
        .futureValue

      // 3 most recently active, newest first
      result shouldBe Vector(pids(4), pids(3), pids(2))
    }

    "order by the latest event timestamp per persistence id" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val pidA = nextPid(entityType)
      val pidB = nextPid(entityType)
      val sliceA = persistenceExt.sliceForPersistenceId(pidA)
      val sliceB = persistenceExt.sliceForPersistenceId(pidB)

      // pidA has an old event and a very recent one; pidB has only a moderately recent event.
      // pidA should rank ahead of pidB because the *latest* event timestamp wins.
      writeEvent(sliceA, pidA, 1L, now.minusSeconds(3600), "old")
      writeEvent(sliceB, pidB, 1L, now.minusSeconds(60), "moderately recent")
      writeEvent(sliceA, pidA, 2L, now.minusSeconds(10), "very recent")

      val result = query
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe Vector(pidA, pidB)
    }

    "filter out persistence ids whose latest event is after toTimestamp" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val withinPids = (1 to 3).map(_ => nextPid(entityType)).toVector
      val afterPids = (1 to 2).map(_ => nextPid(entityType)).toVector

      val withinTime = now.minusSeconds(600)
      val afterTime = now.minusSeconds(60)

      withinPids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, withinTime, "within"))
      afterPids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, afterTime, "after"))

      val toTimestamp = now.minusSeconds(300)
      val result = dao
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromTimestamp = Instant.EPOCH,
          toTimestamp = Some(toTimestamp),
          limit = 100,
          correlationId = None)
        .runWith(Sink.seq)
        .futureValue

      result.toSet shouldBe withinPids.toSet
    }

    "combine fromTimestamp and toTimestamp as a window" in {

      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val beforePids = (1 to 2).map(_ => nextPid(entityType)).toVector
      val withinPids = (1 to 3).map(_ => nextPid(entityType)).toVector
      val afterPids = (1 to 2).map(_ => nextPid(entityType)).toVector

      val beforeTime = now.minusSeconds(3600)
      val withinTime = now.minusSeconds(600)
      val afterTime = now.minusSeconds(10)

      beforePids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, beforeTime, "before"))
      withinPids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, withinTime, "within"))
      afterPids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, afterTime, "after"))

      val fromTimestamp = now.minusSeconds(1800)
      val toTimestamp = now.minusSeconds(120)
      val result = dao
        .persistenceIdsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromTimestamp = fromTimestamp,
          toTimestamp = Some(toTimestamp),
          limit = 100,
          correlationId = None)
        .runWith(Sink.seq)
        .futureValue

      result.toSet shouldBe withinPids.toSet
    }
  }

  "persistenceIdsAndTimestampsBySlices" should {

    pendingIfMoreThanOneDataPartition()
    pendingIfCannotBeTestedWithDialect()

    "return each persistence id paired with the latest db_timestamp" in {
      val entityType = nextEntityType()
      val pid = nextPid(entityType)
      val slice = persistenceExt.sliceForPersistenceId(pid)
      val now = InstantFactory.now()
      val oldTime = now.minusSeconds(3600)
      val recentTime = now.minusSeconds(60)

      writeEvent(slice, pid, 1L, oldTime, "old")
      writeEvent(slice, pid, 2L, recentTime, "recent")

      val result = query
        .persistenceIdsAndTimestampsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe Vector(pid -> recentTime)
    }

    "order results by latest db_timestamp descending, persistence_id ascending as tiebreaker" in {
      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val pids = (1 to 5).map(_ => nextPid(entityType)).toVector
      // assign descending timestamps so the most recently active pid is pids(4)
      val timestamps = pids.indices.map(i => now.minusSeconds((5 - i) * 60L))
      pids.zip(timestamps).foreach { case (pid, ts) =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, ts, "e")
      }

      val result = query
        .persistenceIdsAndTimestampsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe pids.zip(timestamps).reverse
    }

    "use persistence_id ascending as a tiebreaker on equal timestamps" in {
      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val pids = (1 to 4).map(_ => nextPid(entityType)).toVector
      pids.foreach(pid => writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, now, "e"))

      val result = query
        .persistenceIdsAndTimestampsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe pids.sorted.map(pid => pid -> now)
    }

    "return the latest timestamp within the window when an id has events both inside and outside" in {
      val entityType = nextEntityType()
      val pid = nextPid(entityType)
      val slice = persistenceExt.sliceForPersistenceId(pid)
      val now = InstantFactory.now()
      val beforeTime = now.minusSeconds(3600)
      val withinEarly = now.minusSeconds(800)
      val withinLate = now.minusSeconds(400)
      val afterTime = now.minusSeconds(10)

      writeEvent(slice, pid, 1L, beforeTime, "before")
      writeEvent(slice, pid, 2L, withinEarly, "within-early")
      writeEvent(slice, pid, 3L, withinLate, "within-late")
      writeEvent(slice, pid, 4L, afterTime, "after")

      val fromTimestamp = now.minusSeconds(1000)
      val toTimestamp = now.minusSeconds(120)
      val result = query
        .persistenceIdsAndTimestampsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = TimestampOffset(fromTimestamp, Map.empty),
          toOffset = TimestampOffset(toTimestamp, Map.empty),
          limit = 100)
        .runWith(Sink.seq)
        .futureValue

      result shouldBe Vector(pid -> withinLate)
    }

    "respect the limit, truncating from the least-recent end" in {
      val entityType = nextEntityType()
      val now = InstantFactory.now()
      val pids = (1 to 5).map(_ => nextPid(entityType)).toVector
      val timestamps = pids.indices.map(i => now.minusSeconds((5 - i) * 60L))
      pids.zip(timestamps).foreach { case (pid, ts) =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, 1L, ts, "e")
      }

      val result = query
        .persistenceIdsAndTimestampsBySlices(
          entityType,
          minSlice = 0,
          maxSlice = persistenceExt.numberOfSlices - 1,
          fromOffset = NoOffset,
          toOffset = NoOffset,
          limit = 3)
        .runWith(Sink.seq)
        .futureValue

      // 3 most recently active, newest first
      result shouldBe Vector(pids(4) -> timestamps(4), pids(3) -> timestamps(3), pids(2) -> timestamps(2))
    }
  }
}
