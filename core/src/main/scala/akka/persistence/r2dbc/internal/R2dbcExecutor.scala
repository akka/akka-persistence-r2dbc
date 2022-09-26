/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.util.function.BiConsumer
import scala.collection.immutable
import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalStableApi
import akka.dispatch.ExecutionContexts
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.reactivestreams.Publisher
import org.slf4j.Logger
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * INTERNAL API
 */
@InternalStableApi object R2dbcExecutor {
  final implicit class PublisherOps[T](val publisher: Publisher[T]) extends AnyVal {
    def asFuture(): Future[T] =
      Mono.from(publisher).toFuture.toScala

    def asFutureDone(): Future[Done] = {
      val mono: Mono[Done] = Mono.from(publisher).map(_ => Done)
      mono.defaultIfEmpty(Done).toFuture.toScala
    }
  }

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    stmt.execute().asFuture().flatMap { result =>
      result.getRowsUpdated.asFuture().map(_.intValue())(ExecutionContexts.parasitic)
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
    statements.foldLeft(Future.successful(IndexedSeq.empty[Int].toIndexedSeq)) { (acc, stmt) =>
      acc.flatMap { seq =>
        stmt.execute().asFuture().flatMap { res =>
          res.getRowsUpdated.asFuture().map(seq :+ _.intValue())(ExecutionContexts.parasitic)
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
        .collect[scala.collection.mutable.Builder[A, IndexedSeq[A]]](() => immutable.IndexedSeq.newBuilder[A], consumer)
        .map[immutable.IndexedSeq[A]](builder => builder.result().toIndexedSeq)
        .asFuture()
    }
  }
}

/**
 * INTERNAL API:
 */
@InternalStableApi
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
      }(ExecutionContexts.parasitic)
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
   * Run DDL statements in the same transaction.
   */
  def executeDdls(logPrefix: String)(statementFactory: Connection => immutable.IndexedSeq[Statement]): Future[Done] =
    withConnection(logPrefix) { connection =>
      val stmts = statementFactory(connection)
      // connection not intended for concurrent calls, make sure statements are executed one at a time
      stmts.foldLeft(Future.successful[Done](Done)) { (acc, stmt) =>
        acc.flatMap { _ =>
          stmt.execute().asFuture().flatMap { res =>
            res.getRowsUpdated.asFutureDone()
          }
        }
      }
    }

  /**
   * One update statement with auto commit.
   */
  def updateOne(logPrefix: String)(statementFactory: Connection => Statement): Future[Int] =
    withAutoCommitConnection(logPrefix) { connection =>
      updateOneInTx(statementFactory(connection))
    }

  /**
   * Update statement that is constructed by several statements combined with `add()`.
   */
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

  /**
   * One update statement with auto commit and return mapped result. For example with Postgres:
   * {{{
   * INSERT INTO foo(name) VALUES ('bar') returning db_timestamp
   * }}}
   */
  def updateOneReturning[A](
      logPrefix: String)(statementFactory: Connection => Statement, mapRow: Row => A): Future[A] = {
    withAutoCommitConnection(logPrefix) { connection =>
      val stmt = statementFactory(connection)
      stmt.execute().asFuture().flatMap { result =>
        Mono
          .from[A](result.map((row, _) => mapRow(row)))
          .asFuture()
      }
    }
  }

  /**
   * Update statement that is constructed by several statements combined with `add()`. Returns the mapped result of all
   * rows. For example with Postgres:
   * {{{
   * INSERT INTO foo(name) VALUES ('bar') returning db_timestamp
   * }}}
   */
  def updateInBatchReturning[A](logPrefix: String)(
      statementFactory: Connection => Statement,
      mapRow: Row => A): Future[immutable.IndexedSeq[A]] = {
    import scala.collection.JavaConverters._
    withConnection(logPrefix) { connection =>
      val stmt = statementFactory(connection)
      Flux
        .from[Result](stmt.execute())
        .concatMap(_.map((row, _) => mapRow(row)))
        .collectList()
        .asFuture()
        .map(_.iterator().asScala.toVector)
    }
  }

  def selectOne[A](logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[Option[A]] = {
    select(logPrefix)(statement, mapRow).map(_.headOption)
  }

  def select[A](
      logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[immutable.IndexedSeq[A]] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
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
        log.debugN("{} - Select failed: {}", logPrefix, exc)
        connection.close().asFutureDone()
      }

      mappedRows.flatMap { r =>
        connection.close().asFutureDone().map { _ =>
          val durationMicros = durationInMicros(startTime)
          if (durationMicros >= logDbCallsExceedingMicros)
            log.infoN("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, durationMicros)
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
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
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
            log.debugN("{} - DB call failed: {}", logPrefix, exc.toString)
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
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
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
          log.debugN("{} - DB call failed: {}", logPrefix, exc)
          // auto-commit so nothing to rollback
          connection.close().asFutureDone()
        }

        result.flatMap { r =>
          connection.close().asFutureDone().map { _ =>
            val durationMicros = durationInMicros(startTime)
            if (durationMicros >= logDbCallsExceedingMicros)
              log.infoN("{} - DB call completed [{}] in [{}] µs", logPrefix, r, durationMicros)
            r
          }
        }
      }
    }
  }

  private def commitAndClose(connection: Connection): Future[Done] = {
    connection.commitTransaction().asFutureDone().andThen { case _ => connection.close().asFutureDone() }
  }

  private def rollbackAndClose(connection: Connection): Future[Done] = {
    connection.rollbackTransaction().asFutureDone().andThen { case _ => connection.close().asFutureDone() }
  }
}
