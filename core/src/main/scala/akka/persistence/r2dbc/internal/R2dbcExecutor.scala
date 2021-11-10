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
import scala.jdk.FutureConverters.CompletionStageOps
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
import reactor.core.publisher.Mono

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

  final implicit class FluxOps[T](val flux: Flux[T]) extends AnyVal {

    def toIndexedSeq: Mono[immutable.IndexedSeq[T]] = toIndexedSeq(identity)

    def toIndexedSeq[U](map: T => U): Mono[immutable.IndexedSeq[U]] = {
      val consumer: BiConsumer[mutable.Builder[U, IndexedSeq[U]], T] = (builder, elem) => builder += map(elem)
      flux
        .collect(() => IndexedSeq.newBuilder[U], consumer)
        .map(_.result())
    }
  }

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    stmt.execute().asFuture().flatMap { result =>
      result.getRowsUpdated.asFuture().map(_.intValue())(ExecutionContext.parasitic)
    }

  def updateBatchInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    Flux
      .from[Result](stmt.execute())
      .concatMap(_.getRowsUpdated)
      .toIndexedSeq(_.intValue())
      .map(_.sum)
      .asFuture()

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
      system: ActorSystem[_]): Future[immutable.IndexedSeq[A]] =
    statement.execute().asFuture().flatMap { result =>
      Flux
        .from[A](result.map((row, _) => mapRow(row)))
        .toIndexedSeq
        .asFuture()
    }
}

class R2dbcExecutor(val connectionFactory: ConnectionFactory, log: Logger)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import R2dbcExecutor._

  private def getConnection(): Future[Connection] =
    connectionFactory.create().asFuture()

  /**
   * Run DDL statement with auto commit.
   */
  def executeDdl(logPrefix: String)(statement: Connection => Statement): Future[Done] = {
    val startTime = System.nanoTime()

    getConnection().flatMap { connection =>
      connection.setAutoCommit(true).asFutureDone().flatMap { _ =>
        val done =
          try {
            val stmt = statement(connection)
            stmt.execute().asFuture().flatMap { result =>
              result.getRowsUpdated.asFutureDone()
            }
          } catch {
            case NonFatal(exc) =>
              // thrown from statement function
              Future.failed(exc)
          }

        done.failed.foreach { exc =>
          log.debug("{} - DDL failed: {}", logPrefix, exc)
          // auto-commit so nothing to rollback
          connection.close().asFutureDone()
        }

        done.flatMap { _ =>
          connection.close().asFutureDone().map { _ =>
            if (log.isDebugEnabled())
              log.debug("{} - DDL in [{}] µs", logPrefix, (System.nanoTime() - startTime) / 1000)
            Done
          }
        }
      }
    }
  }

  /**
   * One update statement with auto commit.
   */
  def updateOne(logPrefix: String)(statement: Connection => Statement): Future[Int] = {
    val startTime = System.nanoTime()

    getConnection().flatMap { connection =>
      connection.setAutoCommit(true).asFutureDone().flatMap { _ =>
        val rowsUpdated =
          try {
            val boundStmt = statement(connection)
            updateOneInTx(boundStmt)
          } catch {
            case NonFatal(exc) =>
              // thrown from statement function
              Future.failed(exc)
          }

        rowsUpdated.failed.foreach { exc =>
          log.debug("{} - Update failed: {}", logPrefix, exc)
          // auto-commit so nothing to rollback
          connection.close().asFutureDone()
        }

        rowsUpdated.flatMap { r =>
          connection.close().asFutureDone().map { _ =>
            if (log.isDebugEnabled())
              log.debug("{} - Updated [{}] in [{}] µs", logPrefix, r, (System.nanoTime() - startTime) / 1000)
            r
          }
        }
      }
    }
  }

  def updateInBatch(logPrefix: String)(statement: Connection => Statement): Future[Int] =
    withConnection(logPrefix) { connection =>
      val boundStmt = statement(connection)
      updateBatchInTx(boundStmt)
    }

  /**
   * Several update statements in the same transaction.
   */
  def update(logPrefix: String)(
      statements: Connection => immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Int]] = {
    val startTime = System.nanoTime()

    getConnection().flatMap { connection =>
      connection.beginTransaction().asFutureDone().flatMap { _ =>
        val rowsUpdated =
          try {
            val boundStmts = statements(connection)
            updateInTx(boundStmts)
          } catch {
            case NonFatal(exc) =>
              // thrown from statement function
              Future.failed(exc)
          }

        rowsUpdated.failed.foreach { exc =>
          log.debug("{} - Update failed: {}", logPrefix, exc)
          // ok to rollback async like this, or should it be before completing the returned Future?
          rollbackAndClose(connection)
        }

        rowsUpdated.flatMap { r =>
          commitAndClose(connection).map { _ =>
            if (log.isDebugEnabled()) {
              rowsUpdated.foreach { r =>
                log.debug(
                  "{} - Updated [{}] from [{}] statements in [{}] µs",
                  logPrefix,
                  r.sum,
                  r.size,
                  (System.nanoTime() - startTime) / 1000)
              }
            }

            r
          }
        }
      }
    }
  }

  def selectOne[A](logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[Option[A]] = {
    select(logPrefix)(statement, mapRow).map(_.headOption)
  }

  def select[A](
      logPrefix: String)(statement: Connection => Statement, mapRow: Row => A): Future[immutable.IndexedSeq[A]] = {
    val startTime = System.nanoTime()

    getConnection().flatMap { connection =>
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
          if (log.isDebugEnabled())
            log.debug("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, (System.nanoTime() - startTime) / 1000)
          r
        }
      }

    }
  }

  def withConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    val startTime = System.nanoTime()

    getConnection().flatMap { connection =>
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
            if (log.isDebugEnabled())
              log.debug("{} - DB call completed in [{}] µs", logPrefix, (System.nanoTime() - startTime) / 1000)
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
