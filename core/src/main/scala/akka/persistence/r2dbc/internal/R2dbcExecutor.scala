/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger

// FIXME make public?
@InternalStableApi private[akka] object R2dbcExecutor {
  final implicit class PublisherOps[T](val publisher: Publisher[T]) extends AnyVal {
    def asFuture(): Future[T] = {
      val promise = Promise[T]()
      publisher.subscribe(new Subscriber[T] {
        @volatile private var subscription: Subscription = null

        override def onSubscribe(s: Subscription): Unit = {
          subscription = s
          s.request(1)
        }

        override def onNext(value: T): Unit = {
          subscription.cancel()
          subscription = null
          promise.trySuccess(value)
        }

        override def onError(t: Throwable): Unit = {
          subscription = null
          promise.tryFailure(t)
        }

        override def onComplete(): Unit = {
          subscription = null
          if (promise.isCompleted)
            promise.tryFailure(new RuntimeException(s"Publisher [$publisher] completed without first value."))
        }
      })
      promise.future
    }

    def asFutureDone(): Future[Done] = {
      val promise = Promise[Done]()
      publisher.subscribe(new Subscriber[Any] {
        @volatile private var subscription: Subscription = null

        override def onSubscribe(s: Subscription): Unit = {
          subscription = s
          s.request(1)
        }

        override def onNext(value: Any): Unit = {
          subscription.cancel()
          subscription = null
          promise.trySuccess(Done)
        }

        override def onError(t: Throwable): Unit = {
          subscription = null
          promise.tryFailure(t)
        }

        override def onComplete(): Unit = {
          subscription = null
          promise.trySuccess(Done)
        }
      })
      promise.future
    }
  }

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] = {
    stmt.execute().asFuture().flatMap { result =>
      result.getRowsUpdated.asFuture().map(_.intValue())(ExecutionContext.parasitic)
    }
  }

  def updateInTx(statements: immutable.IndexedSeq[Statement])(implicit
      ec: ExecutionContext): Future[immutable.IndexedSeq[Int]] = {
    Future.sequence(statements.map { stmt =>
      // FIXME is it ok to execute next like this before previous has completed?
      stmt.execute().asFuture().flatMap { result =>
        result.getRowsUpdated.asFuture().map(_.intValue())(ExecutionContext.parasitic)
      }
    })
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
      val resultPublisher: Publisher[A] =
        result.map((row, _) => mapRow(row))
      Source.fromPublisher(resultPublisher).runWith(Sink.seq[A]).map(_.toIndexedSeq)(ExecutionContext.parasitic)
    }
  }
}

class R2dbcExecutor(val connectionFactory: ConnectionFactory, log: Logger)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import R2dbcExecutor._

  private def getConnection(): Future[Connection] =
    connectionFactory.create().asFuture()

  def updateOne(logPrefix: String)(statement: Connection => Statement): Future[Int] = {
    val startTime = System.nanoTime()
    val connection = getConnection()

    connection.flatMap { connection =>
      // FIXME is it more efficient to use auto-commit for single updates?
      connection.beginTransaction().asFutureDone().flatMap { _ =>
        val boundStmt =
          try statement(connection)
          catch {
            case NonFatal(exc) =>
              log.debug("{} - Update statement failed: {}", logPrefix, exc)
              rollbackAndClose(connection)
              throw exc
          }
        val rowsUpdated = updateOneInTx(boundStmt)

        if (log.isDebugEnabled()) {
          rowsUpdated.foreach { r =>
            log.debug("{} - Updated [{}] in [{}] µs", logPrefix, r, (System.nanoTime() - startTime) / 1000)
          }
        }

        rowsUpdated.failed.foreach { exc =>
          log.debug("{} - Update failed: {}", logPrefix, exc)
          // ok to rollback async like this, or should it be before completing the returned Future?
          rollbackAndClose(connection)
        }

        rowsUpdated.flatMap { r =>
          commitAndClose(connection).map(_ => r)(ExecutionContext.parasitic)
        }

      }
    }
  }

  def update(logPrefix: String)(
      statements: Connection => immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Int]] = {
    val startTime = System.nanoTime()
    val connection = getConnection()

    connection.flatMap { connection =>
      connection.beginTransaction().asFutureDone().flatMap { _ =>
        val boundStmts =
          try statements(connection)
          catch {
            case NonFatal(exc) =>
              log.debug("{} - Update statement failed: {}", logPrefix, exc)
              rollbackAndClose(connection)
              throw exc
          }
        val rowsUpdated = updateInTx(boundStmts)

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

        rowsUpdated.failed.foreach { exc =>
          log.debug("{} - Update failed: {}", logPrefix, exc)
          // ok to rollback async like this, or should it be before completing the returned Future?
          rollbackAndClose(connection)
        }

        rowsUpdated.flatMap { r =>
          commitAndClose(connection).map(_ => r)(ExecutionContext.parasitic)
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
    val connection = getConnection()

    connection.flatMap { connection =>
      val boundStmt =
        try statement(connection)
        catch {
          case NonFatal(exc) =>
            log.debug("{} - Select statement failed: {}", logPrefix, exc)
            rollbackAndClose(connection)
            throw exc
        }
      val mappedRows = selectInTx(boundStmt, mapRow)

      if (log.isDebugEnabled()) {
        mappedRows.foreach { r =>
          log.debug("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, (System.nanoTime() - startTime) / 1000)
        }
      }

      if (log.isDebugEnabled())
        mappedRows.failed.foreach { exc =>
          log.debug("{} - Select failed: {}", logPrefix, exc)
        }

      mappedRows.andThen { _ =>
        connection.close().asFutureDone()
      }

    }
  }

  def withConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    val startTime = System.nanoTime()
    val connection = getConnection()

    connection.flatMap { connection =>
      connection.beginTransaction().asFutureDone().flatMap { _ =>
        val result =
          try fun(connection)
          catch {
            case NonFatal(exc) =>
              log.debug("{} - Call failed: {}", logPrefix, exc)
              rollbackAndClose(connection)
              throw exc
          }

        if (log.isDebugEnabled()) {
          result.foreach { r =>
            log.debug("{} - DB call completed in [{}] µs", logPrefix, (System.nanoTime() - startTime) / 1000)
          }
        }

        result.failed.foreach { exc =>
          log.debug("{} - DB call failed: {}", logPrefix, exc)
          // ok to rollback async like this, or should it be before completing the returned Future?
          rollbackAndClose(connection)
        }

        result.flatMap { r =>
          commitAndClose(connection).map(_ => r)(ExecutionContext.parasitic)
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
