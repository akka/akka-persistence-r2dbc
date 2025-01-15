/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.BucketDurationSeconds
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BySliceQueryBucketsSpec extends AnyWordSpec with TestSuite with Matchers {

  private val startTime = InstantFactory.now()
  private val firstBucketStartTime = startTime.plusSeconds(60)
  private val firstBucketStartEpochSeconds = firstBucketStartTime.toEpochMilli / 1000

  private def bucketStartEpochSeconds(bucketIndex: Int): Long =
    firstBucketStartEpochSeconds + BucketDurationSeconds * bucketIndex

  private def bucketEndEpochSeconds(bucketIndex: Int): Long =
    bucketStartEpochSeconds(bucketIndex) + BucketDurationSeconds

  private def bucketEndTime(bucketIndex: Int): Instant =
    Instant.ofEpochSecond(bucketEndEpochSeconds(bucketIndex))

  private val buckets = {
    Buckets.empty
      .add(
        List(
          Bucket(bucketStartEpochSeconds(0), 101),
          Bucket(bucketStartEpochSeconds(1), 202),
          Bucket(bucketStartEpochSeconds(2), 303),
          Bucket(bucketStartEpochSeconds(3), 304),
          Bucket(bucketStartEpochSeconds(4), 305),
          Bucket(bucketStartEpochSeconds(5), 306)))
  }

  "BySliceQuery.Buckets" should {
    "find time for events limit" in {
      buckets.findTimeForLimit(startTime, 100) shouldBe Some(bucketEndTime(0))

      // not including the bucket that includes the `from` time
      buckets.findTimeForLimit(firstBucketStartTime, 100) shouldBe Some(bucketEndTime(1))
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(9), 100) shouldBe Some(bucketEndTime(1))
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(10), 100) shouldBe Some(bucketEndTime(2))
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(11), 100) shouldBe Some(bucketEndTime(2))

      // 202 + 303 >= 500
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 500) shouldBe Some(bucketEndTime(2))
      // 202 + 303 >= 505
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 505) shouldBe Some(bucketEndTime(2))
      // 202 + 303 + 304 >= 506
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 506) shouldBe Some(bucketEndTime(3))

      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 1000) shouldBe Some(bucketEndTime(4))
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 1400) shouldBe Some(bucketEndTime(5))
      buckets.findTimeForLimit(firstBucketStartTime.plusSeconds(3), 1500) shouldBe None
    }

    "add buckets with limit of number of buckets" in {
      val manyBuckets = Buckets.empty.add((0 until Buckets.Limit).map(i => Bucket(bucketStartEpochSeconds(i), i)))
      manyBuckets.size shouldBe Buckets.Limit

      val moreBuckets = Buckets.empty.add((0 until Buckets.Limit + 10).map(i => Bucket(bucketStartEpochSeconds(i), i)))
      moreBuckets.size shouldBe Buckets.Limit
    }

    "provide start time for next query" in {
      Buckets.empty
        .add(List(Bucket(bucketStartEpochSeconds(0), 101), Bucket(bucketStartEpochSeconds(1), 202)))
        .nextStartTime shouldBe Some(firstBucketStartTime.truncatedTo(ChronoUnit.SECONDS))

      buckets.nextStartTime shouldBe Some(
        firstBucketStartTime.plusSeconds(4 * BucketDurationSeconds).truncatedTo(ChronoUnit.SECONDS))

      Buckets.empty.nextStartTime shouldBe None

      Buckets.empty
        .add(List(Bucket(bucketStartEpochSeconds(0), 101)))
        .nextStartTime shouldBe None
    }

  }

}
