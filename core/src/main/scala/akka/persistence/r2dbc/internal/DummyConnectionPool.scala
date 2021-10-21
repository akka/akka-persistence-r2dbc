/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.lang
import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.typed.ActorSystem
import io.r2dbc.spi.Batch
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryMetadata
import io.r2dbc.spi.ConnectionMetadata
import io.r2dbc.spi.IsolationLevel
import io.r2dbc.spi.Statement
import io.r2dbc.spi.ValidationDepth
import org.reactivestreams.Publisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

object DummyConnectionPool {
  val log: Logger = LoggerFactory.getLogger(classOf[DummyConnectionPool])
}

// FIXME Couldn't get r2dbc-pool working. Transactions are mixed up. This is just temporary workaround.
class DummyConnectionPool(connectionFactory: ConnectionFactory, max: Int)(implicit val system: ActorSystem[_])
    extends ConnectionFactory {
  import DummyConnectionPool.log
  import akka.persistence.r2dbc.internal.R2dbcExecutor.PublisherOps
  import system.executionContext
  private val pool = new ConcurrentLinkedQueue[PooledConnection]

  (1 to max).foreach { _ =>
    connectionFactory.create().asFuture().foreach(c => pool.offer(new PooledConnection(c, pool)))
  }

  system.whenTerminated.onComplete { _ =>
    var c = pool.poll()
    while (c != null) {
      c.delegate.close().asFuture()
      c = pool.poll()
    }
  }(scala.concurrent.ExecutionContext.global)

  override def create(): Publisher[_ <: Connection] = {
    pool.poll() match {
      case null =>
        Thread.sleep(20)
        pool.poll() match {
          case null =>
            log.warn("creating additional connection outside pool because all connections are busy")
            connectionFactory.create()
          case c =>
            log.debug("using pooled connection after delay, {} remaining", pool.size())
            Mono.just(c)
        }
      case c =>
        log.debug("using pooled connection, {} remaining", pool.size())
        Mono.just(c)
    }
  }

  override def getMetadata: ConnectionFactoryMetadata =
    connectionFactory.getMetadata
}

class PooledConnection(val delegate: Connection, releaseTo: util.Queue[PooledConnection]) extends Connection {
  import DummyConnectionPool.log

  override def beginTransaction(): Publisher[Void] =
    delegate.beginTransaction()

  override def close(): Publisher[Void] = {
    releaseTo.offer(this)
    log.debug("releasing pooled connection, {} available", releaseTo.size())
    Mono.justOrEmpty(null)
  }

  override def commitTransaction(): Publisher[Void] =
    delegate.commitTransaction()

  override def createBatch(): Batch =
    delegate.createBatch()

  override def createSavepoint(name: String): Publisher[Void] =
    delegate.createSavepoint(name)

  override def createStatement(sql: String): Statement =
    delegate.createStatement(sql)

  override def isAutoCommit: Boolean =
    delegate.isAutoCommit

  override def getMetadata: ConnectionMetadata =
    delegate.getMetadata

  override def getTransactionIsolationLevel: IsolationLevel =
    delegate.getTransactionIsolationLevel

  override def releaseSavepoint(name: String): Publisher[Void] =
    delegate.releaseSavepoint(name)

  override def rollbackTransaction(): Publisher[Void] =
    delegate.rollbackTransaction()

  override def rollbackTransactionToSavepoint(name: String): Publisher[Void] =
    delegate.rollbackTransactionToSavepoint(name)

  override def setAutoCommit(autoCommit: Boolean): Publisher[Void] =
    delegate.setAutoCommit(autoCommit)

  override def setTransactionIsolationLevel(isolationLevel: IsolationLevel): Publisher[Void] =
    delegate.setTransactionIsolationLevel(isolationLevel)

  override def validate(depth: ValidationDepth): Publisher[lang.Boolean] =
    delegate.validate(depth)
}
