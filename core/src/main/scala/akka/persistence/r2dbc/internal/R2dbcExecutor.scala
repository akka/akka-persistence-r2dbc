/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer

import scala.collection.immutable
import scala.collection.immutable.IntMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.control.Exception.Catcher
import scala.util.control.NonFatal

import akka.Done
import akka.actor.Cancellable
import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import io.r2dbc.spi.Wrapped
import org.reactivestreams.Publisher
import org.slf4j.Logger
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings

/**
 * INTERNAL API
 */
@InternalStableApi object R2dbcExecutor {
  final implicit class PublisherOps[T](val publisher: Publisher[T]) extends AnyVal {
    def asFuture(): Future[T] =
      Mono.from(publisher).toFuture.asScala

    def asFutureDone(): Future[Done] = {
      val mono: Mono[Done] = Mono.from(publisher).map(_ => Done)
      mono.defaultIfEmpty(Done).toFuture.asScala
    }
  }

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Long] =
    stmt.execute().asFuture().flatMap { result =>
      result.getRowsUpdated.asFuture().map(_.longValue())(ExecutionContext.parasitic)
    }

  def updateOneReturningInTx[A](stmt: Statement, mapRow: Row => A)(implicit ec: ExecutionContext): Future[A] =
    stmt.execute().asFuture().flatMap { result =>
      Mono
        .from[A](result.map((row, _) => mapRow(row)))
        .asFuture()
    }

  def updateBatchInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Long] = {
    val consumer: BiConsumer[Long, java.lang.Long] = (acc, elem) => acc + elem.longValue()
    Flux
      .from[Result](stmt.execute())
      .concatMap(_.getRowsUpdated)
      .collect(() => 0L, consumer)
      .asFuture()
  }

  def updateInTx(statements: immutable.IndexedSeq[Statement])(implicit
      ec: ExecutionContext): Future[immutable.IndexedSeq[Long]] =
    // connection not intended for concurrent calls, make sure statements are executed one at a time
    statements.foldLeft(Future.successful(Vector.empty[Long])) { (acc, stmt) =>
      acc.flatMap { seq =>
        stmt.execute().asFuture().flatMap { res =>
          res.getRowsUpdated.asFuture().map(seq :+ _.longValue())(ExecutionContext.parasitic)
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
class R2dbcExecutor(
    val connectionFactory: ConnectionFactory,
    log: Logger,
    logDbCallsExceeding: FiniteDuration,
    closeCallsExceeding: Option[FiniteDuration],
    closeQueriesExceeding: Option[FiniteDuration])(implicit ec: ExecutionContext, system: ActorSystem[_]) {

  import R2dbcExecutor._

  private val logDbCallsExceedingMicros = logDbCallsExceeding.toMicros
  private val logDbCallsExceedingEnabled = logDbCallsExceedingMicros >= 0

  // for backwards compatibility, closeCallsExceeding should be defined
  @deprecated("Use constructor with closeCallsExceeding and closeQueriesExceeding", "1.2.0")
  def this(connectionFactory: ConnectionFactory, log: Logger, logDbCallsExceeding: FiniteDuration)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]) =
    this(
      connectionFactory,
      log,
      logDbCallsExceeding,
      closeCallsExceeding = Some(20.seconds),
      closeQueriesExceeding = Some(10.minutes))

  // for backwards compatibility, closeQueriesExceeding should be defined
  @deprecated("Use constructor with closeQueriesExceeding", "1.4.2")
  def this(
      connectionFactory: ConnectionFactory,
      log: Logger,
      logDbCallsExceeding: FiniteDuration,
      closeCallsExceeding: Option[FiniteDuration])(implicit ec: ExecutionContext, system: ActorSystem[_]) =
    this(connectionFactory, log, logDbCallsExceeding, closeCallsExceeding, closeQueriesExceeding = Some(10.minutes))

  private def nanoTime(): Long =
    if (logDbCallsExceedingEnabled) System.nanoTime() else 0L

  private def durationInMicros(startTime: Long): Long =
    if (logDbCallsExceedingEnabled) (nanoTime() - startTime) / 1000 else Long.MinValue

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
  def updateOne(logPrefix: String)(statementFactory: Connection => Statement): Future[Long] =
    withAutoCommitConnection(logPrefix) { connection =>
      updateOneInTx(statementFactory(connection))
    }

  /**
   * Update statement that is constructed by several statements combined with `add()`.
   */
  def updateInBatch(logPrefix: String)(statementFactory: Connection => Statement): Future[Long] =
    withConnection(logPrefix) { connection =>
      updateBatchInTx(statementFactory(connection))
    }

  /**
   * Several update statements in the same transaction.
   */
  def update(logPrefix: String)(
      statementsFactory: Connection => immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Long]] =
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
      updateOneReturningInTx(statementFactory(connection), mapRow)
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
    import scala.jdk.CollectionConverters._
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
      logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[immutable.IndexedSeq[A]] =
    select(logPrefix, closeQueriesExceeding)(statement, mapRow)

  /**
   * Like `select`, with the time limit `timeout` instead of `closeQueriesExceeding`. `None` means no time limit.
   */
  def select[A](logPrefix: String, timeout: Option[FiniteDuration])(
      statement: Connection => Statement,
      mapRow: Row => A): Future[immutable.IndexedSeq[A]] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
      val call = new CallTimeout(logPrefix, connection, timeout)

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
        call.close()
      }

      mappedRows.flatMap { r =>
        call.close().map { _ =>
          val durationMicros = durationInMicros(startTime)
          if (durationMicros >= logDbCallsExceedingMicros)
            log.info("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, durationMicros)
          r
        }
      }

    }
  }

  /**
   * Runs the passed function in using a Connection with a new transaction. The connection is closed and the transaction
   * is committed at the end or rolled back in case of failures.
   */
  def withConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
      val call = new CallTimeout(logPrefix, connection, closeCallsExceeding)

      val begin = futureDone(connection.beginTransaction())

      begin.failed.foreach { exc =>
        if (log.isDebugEnabled())
          log.debug("{} - Begin transaction failed: {}", logPrefix, exc.toString)
        // the transaction was not started, so there is nothing to roll back
        call.close()
      }

      begin.flatMap { _ =>
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
          call.rollbackAndClose()
        }

        result.flatMap { r =>
          call.commitAndClose().map { _ =>
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
      val call = new CallTimeout(logPrefix, connection, closeCallsExceeding)

      val autoCommit = futureDone(connection.setAutoCommit(true))

      autoCommit.failed.foreach { exc =>
        if (log.isDebugEnabled())
          log.debug("{} - Set auto-commit failed: {}", logPrefix, exc.toString)
        call.close()
      }

      autoCommit.flatMap { _ =>
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
          call.close()
        }

        result.flatMap { r =>
          call.close().map { _ =>
            val durationMicros = durationInMicros(startTime)
            if (durationMicros >= logDbCallsExceedingMicros)
              log.info("{} - DB call completed [{}] in [{}] µs", logPrefix, r, durationMicros)
            r
          }
        }
      }
    }
  }

  // an exception thrown by the publisher factory becomes a failed Future
  private def futureDone(publisher: => Publisher[Void]): Future[Done] =
    try publisher.asFutureDone()
    catch {
      case NonFatal(exc) => Future.failed(exc)
    }

  /**
   * INTERNAL API: ends a call once, by the caller or by the timer of the time limit, whichever comes first. The one
   * that ends the call closes the pooled connection.
   */
  private[internal] final class CallTimeout(
      logPrefix: String,
      connection: Connection,
      timeout: Option[FiniteDuration]) {
    private val ended = new AtomicBoolean(false)

    private val task: Option[Cancellable] =
      timeout.map(t => system.scheduler.scheduleOnce(t, () => onTimeout(t)))

    /**
     * Closes the pooled connection and cancels the timer. Does nothing if the timer has ended the call, because the
     * timer closes the pooled connection then.
     */
    def close(): Future[Done] =
      if (ended.compareAndSet(false, true)) {
        // The timer must not close the connection after this. The pool can hand it to another caller as soon as it is
        // released. The executor never closes the connection while a transaction is open, so PooledConnection.close()
        // sends nothing to the server and does not need the timer.
        task.foreach(_.cancel())
        try connection.close().asFutureDone()
        catch ignoreConnectionClosedException
      } else
        Future.successful(Done)

    /**
     * Commits the transaction and then closes the pooled connection. Completes when the commit completes.
     */
    def commitAndClose(): Future[Done] =
      futureDone(connection.commitTransaction()).andThen { case _ => close() }

    /**
     * Rolls back the transaction and then closes the pooled connection.
     */
    def rollbackAndClose(): Unit =
      futureDone(connection.rollbackTransaction()).onComplete(_ => close())

    // the action of the timer, package private for tests
    private[internal] def onTimeout(t: FiniteDuration): Unit =
      if (ended.compareAndSet(false, true))
        closeAfterTimeout(logPrefix, connection, t)
  }

  private def closeAfterTimeout(logPrefix: String, connection: Connection, timeout: FiniteDuration): Unit = {
    log.warn("{} - Timeout after [{}], will try to close connection", logPrefix, timeout.toCoarsest)

    // Closing the driver connection fails the pending operations of the call. The drivers report the connection as not
    // valid as soon as the close has started, so the pooled connection is closed right after that, without waiting for
    // the driver close. That close does not complete while the socket accepts no more writes. The pool discards the
    // connection, because it is not valid, and the slot comes back also when the caller does not complete.
    connection match {
      case wrapped: Wrapped[Connection] @unchecked =>
        futureDone(wrapped.unwrap().close()).failed.foreach { exc =>
          log.debug("{} - Tried to close driver connection after timeout, but {}", logPrefix, exc.toString)
        }
      case _ =>
    }

    val done =
      try connection.close().asFutureDone()
      catch ignoreConnectionClosedException

    done.onComplete {
      case Success(_) =>
        log.debug("{} - Connection closed after timeout", logPrefix)
      case Failure(exc) =>
        log.debug("{} - Tried to close connection after timeout, but {}", logPrefix, exc.toString)
    }
  }

  private def ignoreConnectionClosedException: Catcher[Future[Done]] = { case _: IllegalStateException =>
    // thrown by PooledConnection.assertNotClosed when the connection is already closed
    Future.successful(Done)
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi class R2dbcExecutorProvider(
    val system: ActorSystem[_],
    val ec: ExecutionContext,
    val settings: R2dbcSettings,
    connectionFactoryBaseConfigPath: String,
    log: Logger) {
  private val connectionFactoryProvider = ConnectionFactoryProvider(system)
  private var cache = IntMap.empty[R2dbcExecutor]

  def executorFor(slice: Int): R2dbcExecutor = {
    cache.get(slice) match {
      case Some(executor) => executor
      case None =>
        val connectionFactoryConfigPath =
          settings.resolveConnectionFactoryConfigPath(connectionFactoryBaseConfigPath, slice)
        val connectionFactory = connectionFactoryProvider.connectionFactoryFor(connectionFactoryConfigPath)
        val executor = new R2dbcExecutor(
          connectionFactory,
          log,
          settings.logDbCallsExceeding,
          settings.connectionFactorySettings.poolSettings.closeCallsExceeding,
          settings.connectionFactorySettings.poolSettings.closeQueriesExceeding)(ec, system)
        // it's just a cache so no need for guarding concurrent updates or visibility
        cache = cache.updated(slice, executor)
        executor
    }
  }

}
