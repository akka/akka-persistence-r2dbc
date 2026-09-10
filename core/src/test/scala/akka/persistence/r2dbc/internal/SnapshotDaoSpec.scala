/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SnapshotDaoSpec extends AnyWordSpec with Matchers {

  // only sequenceNumberOfSnapshot and maxConcurrentSequenceNumberLookups are exercised by
  // sequenceNumbersOfSnapshotsConcurrently, the default `sequenceNumbersOfSnapshots` implementation under test here
  private class TestDao(maxConcurrent: Int) extends SnapshotDao {
    protected implicit def ec: ExecutionContext = ExecutionContext.global
    protected def maxConcurrentSequenceNumberLookups: Int = maxConcurrent

    private val active = new AtomicInteger(0)
    val maxObservedConcurrency = new AtomicInteger(0)
    val invoked = new LinkedBlockingQueue[(String, Promise[Option[Long]])]()

    override def sequenceNumberOfSnapshot(persistenceId: String): Future[Option[Long]] = {
      maxObservedConcurrency.updateAndGet(prev => math.max(prev, active.incrementAndGet()))
      val promise = Promise[Option[Long]]()
      promise.future.onComplete(_ => active.decrementAndGet())(ExecutionContext.parasitic)
      invoked.put(persistenceId -> promise)
      promise.future
    }

    override def load(
        persistenceId: String,
        criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]] =
      throw new UnsupportedOperationException
    override def store(serializedRow: SerializedSnapshotRow): Future[Unit] = throw new UnsupportedOperationException
    override def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
      throw new UnsupportedOperationException

    // takes the next `n` invocations off the queue without completing them
    def takeNext(n: Int): Seq[(String, Promise[Option[Long]])] =
      (1 to n).map(_ => invoked.poll(3, TimeUnit.SECONDS))

    // completes the next `n` not-yet-completed invocations, one at a time, in the order sequenceNumberOfSnapshot
    // was called for them. Interleaving poll-then-complete (rather than pre-taking a whole wave) lets a chunk
    // whose size doesn't evenly divide `n` progress correctly.
    def completeNext(n: Int)(result: String => Option[Long]): Unit =
      takeNext(n).foreach { case (persistenceId, promise) => promise.success(result(persistenceId)) }

    def assertNothingInvokedWithin(duration: FiniteDuration): Unit =
      invoked.poll(duration.toMillis, TimeUnit.MILLISECONDS) shouldBe null
  }

  "SnapshotDao.sequenceNumbersOfSnapshots default implementation" should {
    "resolve all persistence ids, keeping only the ones with a snapshot" in {
      val dao = new TestDao(maxConcurrent = 2)
      val ids = (1 to 5).map(i => s"pid-$i").toSet
      val resultFuture = dao.sequenceNumbersOfSnapshots(ids)

      // every id gets a snapshot except pid-3
      ids.toList.foreach(_ => dao.completeNext(1)(persistenceId => if (persistenceId == "pid-3") None else Some(7L)))

      val result = Await.result(resultFuture, 3.seconds)
      result shouldBe (ids - "pid-3").map(_ -> 7L).toMap
    }

    "never have more than maxConcurrentSequenceNumberLookups lookups in flight at once" in {
      val dao = new TestDao(maxConcurrent = 3)
      val ids = (1 to 7).map(i => s"pid-$i").toSet
      val resultFuture = dao.sequenceNumbersOfSnapshots(ids)

      // wave 1: exactly maxConcurrent in flight, nothing more until they complete
      val wave1 = dao.takeNext(3)
      dao.assertNothingInvokedWithin(200.millis)
      wave1.foreach { case (_, promise) => promise.success(Some(1L)) }

      // wave 2: same check
      val wave2 = dao.takeNext(3)
      dao.assertNothingInvokedWithin(200.millis)
      wave2.foreach { case (_, promise) => promise.success(Some(1L)) }

      // wave 3: the trailing, smaller-than-maxConcurrent chunk
      dao.takeNext(1).foreach { case (_, promise) => promise.success(Some(1L)) }

      Await.result(resultFuture, 3.seconds) shouldBe ids.map(_ -> 1L).toMap
      dao.maxObservedConcurrency.get() shouldBe 3
    }
  }
}
