/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
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
  private val firstBucketStartEpochSeconds = firstBucketStartTime.getEpochSecond

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

    "clear until time" in {
      buckets.clearUntil(startTime).size shouldBe buckets.size
      buckets.clearUntil(firstBucketStartTime).size shouldBe buckets.size
      buckets.clearUntil(firstBucketStartTime.plusSeconds(9)).size shouldBe buckets.size

      buckets.clearUntil(firstBucketStartTime.plusSeconds(10)).size shouldBe buckets.size - 1
      buckets.clearUntil(firstBucketStartTime.plusSeconds(11)).size shouldBe buckets.size - 1
      buckets.clearUntil(firstBucketStartTime.plusSeconds(19)).size shouldBe buckets.size - 1

      buckets.clearUntil(firstBucketStartTime.plusSeconds(31)).size shouldBe buckets.size - 3
      buckets.clearUntil(firstBucketStartTime.plusSeconds(100)).size shouldBe 1 // keep last

      // don't change createdAt
      buckets.clearUntil(firstBucketStartTime.plusSeconds(31)).createdAt shouldBe buckets.createdAt

      // don't change hasMore
      buckets.hasMore shouldBe false
      val bucketsWithMore = buckets.add(Nil, hasMore = true)
      bucketsWithMore.clearUntil(firstBucketStartTime.plusSeconds(31)).hasMore shouldBe true
      bucketsWithMore.clearUntil(firstBucketStartTime.plusSeconds(100)).hasMore shouldBe true
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
        .nextStartTime shouldBe Some(
        firstBucketStartTime.minusSeconds(Buckets.BucketDurationSeconds).truncatedTo(ChronoUnit.SECONDS))
    }

    "append empty bucket at the end of time range" in {
      // reproducer of rounding bug when the timestamp is at the end of the bucket
      val toTimestamp = Instant.parse("2025-12-02T08:55:39.508Z")
      val lastBucketStartTime = 1764665730L
      Instant.ofEpochSecond(lastBucketStartTime) shouldBe Instant.parse("2025-12-02T08:55:30Z")

      Buckets.appendEmptyBucketIfLastIsMissing(Vector.empty, toTimestamp) shouldBe
      Vector(Bucket(lastBucketStartTime, 0))

      val earlierBucket = Bucket(lastBucketStartTime - 10 * BucketDurationSeconds, 2)
      Buckets.appendEmptyBucketIfLastIsMissing(Vector(earlierBucket), toTimestamp) shouldBe
      Vector(earlierBucket, Bucket(lastBucketStartTime, 0))

      // already has a bucket for the end of the time range
      val lastBucket = Bucket(lastBucketStartTime, 2)
      Buckets.appendEmptyBucketIfLastIsMissing(Vector(earlierBucket, lastBucket), toTimestamp) shouldBe
      Vector(earlierBucket, lastBucket)

      // already has a bucket after the end of the time range, don't replace the count of the lastBucket
      val laterBucket = Bucket(lastBucketStartTime + BucketDurationSeconds, 3)
      Buckets.appendEmptyBucketIfLastIsMissing(Vector(earlierBucket, lastBucket, laterBucket), toTimestamp) shouldBe
      Vector(earlierBucket, lastBucket, laterBucket)
      Buckets.appendEmptyBucketIfLastIsMissing(Vector(earlierBucket, laterBucket), toTimestamp) shouldBe
      Vector(earlierBucket, laterBucket)
    }

    "limit time range of bucket count query" in {
      val fromTimestamp = Instant.parse("2025-12-02T08:38:49.508Z")
      val fromBucketStartTime = 1764664720L
      Instant.ofEpochSecond(fromBucketStartTime) shouldBe Instant.parse("2025-12-02T08:38:40Z")
      val now = fromTimestamp.plusSeconds(3 * 24 * 3600)
      val limit = 100

      // limit + 1 buckets after fromTimestamp
      val toTimestamp = Buckets.countBucketsToTimestamp(fromTimestamp, limit, now)
      toTimestamp shouldBe fromTimestamp.plusSeconds((limit + 1) * BucketDurationSeconds)
      toTimestamp shouldBe Instant.parse("2025-12-02T08:55:39.508Z")
      Buckets.appendEmptyBucketIfLastIsMissing(Vector.empty, toTimestamp).last.startTime shouldBe
      (fromBucketStartTime + (limit + 1) * BucketDurationSeconds)

      Buckets.countBucketsToTimestamp(fromTimestamp, Buckets.Limit, now) shouldBe
      fromTimestamp.plusSeconds((Buckets.Limit + 1) * BucketDurationSeconds)

      // not after now
      Buckets.countBucketsToTimestamp(now.minusSeconds(limit * BucketDurationSeconds), limit, now) shouldBe now
      Buckets.countBucketsToTimestamp(now.minusSeconds(1), limit, now) shouldBe now
      Buckets.countBucketsToTimestamp(now, limit, now) shouldBe now

      // no time range limit from the beginning of time
      Buckets.countBucketsToTimestamp(Instant.EPOCH, limit, now) shouldBe now
    }

  }

}
