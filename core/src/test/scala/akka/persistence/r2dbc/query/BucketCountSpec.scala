/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.InstantFactory

class BucketCountSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val dao = settings.connectionFactorySettings.dialect.createQueryDao(r2dbcExecutorProvider)

  "BySliceQuery.Dao" should {

    "count events in 10 second buckets" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = persistenceExt.sliceForPersistenceId(pid1)
      val slice2 = persistenceExt.sliceForPersistenceId(pid2)

      val startTime = InstantFactory.now().minusSeconds(3600)
      // db epoch seconds is rounding, but Instant is not
      val bucketStartTime = (startTime.plusMillis(500).getEpochSecond / 10) * 10

      (0 until 10).foreach { i =>
        writeEvent(slice1, pid1, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
        writeEvent(slice2, pid2, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
      }

      val buckets =
        dao
          .countBuckets(entityType, 0, persistenceExt.numberOfSlices - 1, startTime, Buckets.Limit, None)
          .futureValue
      withClue(s"startTime $startTime ($bucketStartTime): ") {
        buckets.size shouldBe 10
        buckets.head.startTime shouldBe bucketStartTime
        buckets.last.startTime shouldBe (bucketStartTime + 9 * Buckets.BucketDurationSeconds)
        buckets.map(_.count).toSet shouldBe Set(2)
        buckets.map(_.count).sum shouldBe (2 * 10)
      }
    }

    "append empty bucket if no events in the last bucket, limit before now" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = persistenceExt.sliceForPersistenceId(pid1)
      val slice2 = persistenceExt.sliceForPersistenceId(pid2)

      val limit = 100
      val startTime = InstantFactory.now().minusSeconds(3600)
      // db epoch seconds is rounding, but Instant is not
      val bucketStartTime = (startTime.plusMillis(500).getEpochSecond / 10) * 10

      (0 until 10).foreach { i =>
        writeEvent(slice1, pid1, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
        writeEvent(slice2, pid2, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
      }

      val buckets =
        dao
          .countBuckets(entityType, 0, persistenceExt.numberOfSlices - 1, startTime, limit, None)
          .futureValue
      withClue(s"startTime $startTime ($bucketStartTime): ") {
        buckets.size shouldBe 11
        buckets.head.startTime shouldBe bucketStartTime
        // the toTimestamp of the sql query is one bucket more than fromTimestamp + (limit * BucketDurationSeconds)
        buckets.last.startTime shouldBe (bucketStartTime + (limit + 1) * Buckets.BucketDurationSeconds)
        buckets.last.count shouldBe 0
        buckets.dropRight(1).map(_.count).toSet shouldBe Set(2)
        buckets.map(_.count).sum shouldBe (2 * 10)
      }
    }

    // reproducer of bucket timestamp rounding bug with "real" test data
    "count events in 10 second buckets for interesting timestamp pattern" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = "deal"

      val startTime = Instant.parse("2025-04-30T14:54:41.569186Z")

      TestData.oldEventsWithInterestingTimestampPattern.foreach { case (t, pid, seqNr) =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, seqNr, Instant.parse(t), s"e-$seqNr")
      }

      val buckets =
        dao
          .countBuckets(entityType, 960, 975, startTime, Buckets.Limit, None)
          .futureValue

      buckets.head.startTime shouldBe 1746024900L
      buckets.head.count shouldBe 36

      buckets.find(_.startTime == 1746026590L).get.count shouldBe 10L
      buckets.find(_.startTime == 1746026600L) shouldBe None
      buckets.find(_.startTime == 1746026610L).get.count shouldBe 105L
      buckets.find(_.startTime == 1746026620L) shouldBe None
      buckets.find(_.startTime == 1746026630L).get.count shouldBe 2L

      (1746024900L to 1746026700L by 10).foreach { epochSeconds =>
        val count = r2dbcExecutor
          .selectOne("select count")(
            _.createStatement(
              s"select count(*) from ${settings.journalTableWithSchema(960)} where entity_type = 'deal' " +
              s"and slice BETWEEN 960 AND 975 " +
              s"and db_timestamp >= '${Instant.ofEpochSecond(epochSeconds)}' " +
              s"and db_timestamp < '${Instant.ofEpochSecond(epochSeconds + 10)}' "),
            _.get(0, classOf[java.lang.Long]).longValue())
          .futureValue
          .getOrElse(0L)
        if (count == 0L)
          buckets.find(_.startTime == epochSeconds) shouldBe None
        else
          buckets.find(_.startTime == epochSeconds).get.count shouldBe count
      }

    }

  }

}
