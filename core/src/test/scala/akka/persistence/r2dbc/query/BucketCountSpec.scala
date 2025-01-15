/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

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
      val bucketStartTime = (startTime.getEpochSecond / 10) * 10

      (0 until 10).foreach { i =>
        writeEvent(slice1, pid1, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
        writeEvent(slice2, pid2, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
      }

      val buckets =
        dao
          .countBuckets(entityType, 0, persistenceExt.numberOfSlices - 1, startTime, Buckets.Limit)
          .futureValue
      withClue(s"startTime $startTime ($bucketStartTime): ") {
        buckets.size shouldBe 10
        buckets.head.startTime shouldBe bucketStartTime
        buckets.last.startTime shouldBe (bucketStartTime + 9 * Buckets.BucketDurationSeconds)
        buckets.map(_.count).toSet shouldBe Set(2)
        buckets.map(_.count).sum shouldBe (2 * 10)
      }
    }

    "append 2 empty buckets if no events in the last buckets, before now" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = persistenceExt.sliceForPersistenceId(pid1)
      val slice2 = persistenceExt.sliceForPersistenceId(pid2)

      val limit = 100
      val startTime = InstantFactory.now().minusSeconds(3600)
      val bucketStartTime = (startTime.getEpochSecond / 10) * 10

      (0 until 10).foreach { i =>
        writeEvent(slice1, pid1, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
        writeEvent(slice2, pid2, 1 + i, startTime.plusSeconds(Buckets.BucketDurationSeconds * i), s"e1-$i")
      }

      val buckets =
        dao
          .countBuckets(entityType, 0, persistenceExt.numberOfSlices - 1, startTime, limit)
          .futureValue
      withClue(s"startTime $startTime ($bucketStartTime): ") {
        buckets.size shouldBe 12
        buckets.head.startTime shouldBe bucketStartTime
        // the toTimestamp of the sql query is one bucket more than fromTimestamp + (limit * BucketDurationSeconds)
        buckets.last.startTime shouldBe (bucketStartTime + (limit + 1) * Buckets.BucketDurationSeconds)
        buckets.last.count shouldBe 0
        buckets.dropRight(1).last.startTime shouldBe (bucketStartTime + limit * Buckets.BucketDurationSeconds)
        buckets.dropRight(1).last.count shouldBe 0
        buckets.map(_.count).toSet shouldBe Set(2, 0)
        buckets.map(_.count).sum shouldBe (2 * 10)
      }
    }

  }

}
