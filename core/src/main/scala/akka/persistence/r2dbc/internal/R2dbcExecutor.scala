/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.util.function.BiConsumer

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger
import reactor.core.publisher.Flux

/**
 * INTERNAL API
 */
@InternalStableApi object R2dbcExecutor {
  final implicit class PublisherOps[T](val publisher: Publisher[T]) extends AnyVal {
    def asFuture(): Future[T] = {
      val promise = Promise[T]()
      publisher.subscribe(new Subscriber[T] {

        override def onSubscribe(s: Subscription): Unit = {
          s.request(1)
        }

        override def onNext(value: T): Unit = {
          promise.trySuccess(value)
        }

        override def onError(t: Throwable): Unit = {
          promise.tryFailure(t)
        }

        override def onComplete(): Unit = {
          if (!promise.isCompleted)
            promise.tryFailure(new RuntimeException(s"Publisher [$publisher] completed without first value."))
        }
      })
      promise.future
    }

    def asFutureDone(): Future[Done] = {
      val promise = Promise[Done]()
      publisher.subscribe(new Subscriber[Any] {

        override def onSubscribe(s: Subscription): Unit = {
          s.request(1)
        }

        override def onNext(value: Any): Unit = {
          promise.trySuccess(Done)
        }

        override def onError(t: Throwable): Unit = {
          promise.tryFailure(t)
        }

        override def onComplete(): Unit = {
          promise.trySuccess(Done)
        }
      })
      promise.future
    }
  }

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    stmt.execute().asFuture().flatMap { result =>
      result.getRowsUpdated.asFuture().map(_.intValue())(ExecutionContext.parasitic)
    }

  def updateBatchInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] = {
    val consumer: BiConsumer[Int, Integer] = (acc, elem) => acc + elem.intValue()
    Flux
      .from[Result](stmt.execute())
      .concatMap(_.getRowsUpdated)
      .collect(() => 0, consumer)
      .asFuture()
  }

  def updateInTx(statements: immutable.IndexedSeq[Statement])(implicit
      ec: ExecutionContext): Future[immutable.IndexedSeq[Int]] =
    // connection not intended for concurrent calls, make sure statements are executed one at a time
    statements.foldLeft(Future.successful(IndexedSeq.empty[Int])) { (acc, stmt) =>
      acc.flatMap { seq =>
        stmt.execute().asFuture().flatMap { res =>
          res.getRowsUpdated.asFuture().map(seq :+ _.intValue())(ExecutionContext.parasitic)
        }
      }
    }

  def selectOneInTx[A](statement: Statement, mapRow: Row => A)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): Future[Option[A]] = {
    selectInTx(statement, mapRow).map(_.headOption)
  }

  def selectInTx[A](statement: Statement, mapRow: Row => A)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): Future[immutable.IndexedSeq[A]] = {
    statement.execute().asFuture().flatMap { result =>
      val consumer: BiConsumer[mutable.Builder[A, IndexedSeq[A]], A] = (builder, elem) => builder += elem
      Flux
        .from[A](result.map((row, _) => mapRow(row)))
        .collect(() => IndexedSeq.newBuilder[A], consumer)
        .map(_.result())
        .asFuture()
    }
  }
}

class R2dbcExecutor(val connectionFactory: ConnectionFactory, log: Logger, logDbCallsExceeding: FiniteDuration)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import R2dbcExecutor._

  private val logDbCallsExceedingMicros = logDbCallsExceeding.toMicros
  private val logDbCallsExceedingEnabled = logDbCallsExceedingMicros >= 0

  private def nanoTime(): Long =
    if (logDbCallsExceedingEnabled) System.nanoTime() else 0L

  private def durationInMicros(startTime: Long): Long =
    (nanoTime() - startTime) / 1000

  private def getConnection(logPrefix: String): Future[Connection] = {
    val startTime = nanoTime()
    connectionFactory
      .create()
      .asFuture()
      .map { connection =>
        val durationMicros = durationInMicros(startTime)
        if (durationMicros >= logDbCallsExceedingMicros)
          log.info("{} - getConnection took [{}] µs", logPrefix, durationMicros)
        connection
      }(ExecutionContext.parasitic)
  }

  /**
   * Run DDL statement with auto commit.
   */
  def executeDdl(logPrefix: String)(statement: Connection => Statement): Future[Done] =
    withAutoCommitConnection(logPrefix) { connection =>
      val stmt = statement(connection)
      stmt.execute().asFuture().flatMap { result =>
        result.getRowsUpdated.asFutureDone()
      }
    }

  /**
   * One update statement with auto commit.
   */
  def updateOne(logPrefix: String)(statementFactory: Connection => Statement): Future[Int] =
    withAutoCommitConnection(logPrefix) { connection =>
      updateOneInTx(statementFactory(connection))
    }

  def updateInBatch(logPrefix: String)(statementFactory: Connection => Statement): Future[Int] =
    withConnection(logPrefix) { connection =>
      updateBatchInTx(statementFactory(connection))
    }

  /**
   * Several update statements in the same transaction.
   */
  def update(logPrefix: String)(
      statementsFactory: Connection => immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Int]] =
    withConnection(logPrefix) { connection =>
      updateInTx(statementsFactory(connection))
    }

  def selectOne[A](logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[Option[A]] = {
    select(logPrefix)(statement, mapRow).map(_.headOption)
  }

  def select[A](
      logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[immutable.IndexedSeq[A]] = {
    val startTime = nanoTime()

    getConnection(logPrefix).flatMap { connection =>
      val mappedRows =
        try {
          val boundStmt = statement(connection)
          selectInTx(boundStmt, mapRow)
        } catch {
          case NonFatal(exc) =>
            // thrown from statement function
            Future.failed(exc)
        }

      mappedRows.failed.foreach { exc =>
        log.debug("{} - Select failed: {}", logPrefix, exc)
        connection.close().asFutureDone()
      }

      mappedRows.flatMap { r =>
        connection.close().asFutureDone().map { _ =>
          val durationMicros = durationInMicros(startTime)
          if (durationMicros >= logDbCallsExceedingMicros)
            log.info("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, durationMicros)
          r
        }
      }

    }
  }

  /**
   * Runs the passed function in using a Connection that's participating on a transaction Transaction is commit at the
   * end or rolled back in case of failures.
   */
  def withConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    val startTime = nanoTime()

    getConnection(logPrefix).flatMap { connection =>
      connection.beginTransaction().asFutureDone().flatMap { _ =>
        val result =
          try {
            fun(connection)
          } catch {
            case NonFatal(exc) =>
              // thrown from statement function
              Future.failed(exc)
          }

        result.failed.foreach { exc =>
          if (log.isDebugEnabled())
            log.debug("{} - DB call failed: {}", logPrefix, exc.toString)
          // ok to rollback async like this, or should it be before completing the returned Future?
          rollbackAndClose(connection)
        }

        result.flatMap { r =>
          commitAndClose(connection).map { _ =>
            val durationMicros = durationInMicros(startTime)
            if (durationMicros >= logDbCallsExceedingMicros)
              log.info("{} - DB call completed in [{}] µs", logPrefix, durationMicros)
            r
          }
        }

      }
    }
  }

  /**
   * Runs the passed function in using a Connection with auto-commit enable (non-transactional).
   */
  def withAutoCommitConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    val startTime = nanoTime()

    getConnection(logPrefix).flatMap { connection =>
      connection.setAutoCommit(true).asFutureDone().flatMap { _ =>
        val result =
          try {
            fun(connection)
          } catch {
            case NonFatal(exc) =>
              // thrown from statement function
              Future.failed(exc)
          }

        result.failed.foreach { exc =>
          log.debug("{} - DB call failed: {}", logPrefix, exc)
          // auto-commit so nothing to rollback
          connection.close().asFutureDone()
        }

        result.flatMap { r =>
          connection.close().asFutureDone().map { _ =>
            val durationMicros = durationInMicros(startTime)
            if (durationMicros >= logDbCallsExceedingMicros)
              log.info("{} - DB call completed [{}] in [{}] µs", logPrefix, r, durationMicros)
            r
          }
        }
      }
    }
  }

  private def commitAndClose(connection: Connection): Future[Done] = {
    connection.commitTransaction().asFutureDone().andThen(_ => connection.close().asFutureDone())
  }

  private def rollbackAndClose(connection: Connection): Future[Done] = {
    connection.rollbackTransaction().asFutureDone().andThen(_ => connection.close().asFutureDone())
  }
}
