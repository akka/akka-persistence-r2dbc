/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
import akka.persistence.r2dbc.ConnectionPoolSettings
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.internal.R2dbcExecutor.PublisherOps
import com.typesafe.config.ConfigFactory
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.Batch
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryMetadata
import io.r2dbc.spi.ConnectionMetadata
import io.r2dbc.spi.IsolationLevel
import io.r2dbc.spi.R2dbcNonTransientResourceException
import io.r2dbc.spi.Result
import io.r2dbc.spi.Statement
import io.r2dbc.spi.TransactionDefinition
import io.r2dbc.spi.ValidationDepth
import org.reactivestreams.Publisher
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink

object ConnectionFailureSpec {

  sealed trait Fault

  object Fault {

    /** `create()` never emits and never completes. */
    case object CreateHangs extends Fault

    /** `beginTransaction()` fails at once. */
    case object BeginFails extends Fault

    /** `beginTransaction()` does not complete until the connection is closed. */
    case object BeginHangs extends Fault

    /** `setAutoCommit()` fails at once. */
    case object AutoCommitFails extends Fault

    /** The first statement that is not the validation query does not complete until the connection is closed. */
    case object QueryHangs extends Fault

    /** The statement for the validation query does not complete until the connection is closed. */
    case object ValidationHangs extends Fault
  }

  /**
   * Hands out connections in the order of the planned faults. A connection with no planned fault behaves normally.
   */
  final class FaultyConnectionFactory(delegate: ConnectionFactory, validationQuery: String) extends ConnectionFactory {
    private val plannedFaults = new ConcurrentLinkedQueue[Fault]
    private val created = new ConcurrentLinkedQueue[FaultyConnection]

    def createdConnections: Vector[FaultyConnection] = created.asScala.toVector

    def plan(faults: Fault*): Unit =
      faults.foreach(plannedFaults.add)

    // the pool subscribes to the same publisher for each new connection
    override def create(): Publisher[Connection] =
      Mono.defer { () =>
        Option(plannedFaults.poll()) match {
          case Some(Fault.CreateHangs) =>
            Mono.never[Connection]()
          case fault =>
            Mono.from(delegate.create()).map[Connection] { connection =>
              val faultyConnection = new FaultyConnection(connection, fault, validationQuery)
              created.add(faultyConnection)
              faultyConnection
            }
        }
      }

    override def getMetadata: ConnectionFactoryMetadata = delegate.getMetadata
  }

  /**
   * A fault applies once, to the first operation of its kind, because the pool can hand out the same connection again.
   * The executor closes this connection when a call times out, because `PooledConnection.unwrap()` returns it.
   */
  final class FaultyConnection(delegate: Connection, fault: Option[Fault], validationQuery: String) extends Connection {
    import Fault._

    private val faultApplied = new AtomicBoolean(false)
    private val closed = new AtomicBoolean(false)
    private val hungOperations = new ConcurrentLinkedQueue[MonoSink[_]]

    def isClosed: Boolean = closed.get()

    private def applyFault(f: Fault): Boolean =
      fault.contains(f) && faultApplied.compareAndSet(false, true)

    private def connectionClosedException() =
      new R2dbcNonTransientResourceException("Connection closed")

    private def hang[T](): Mono[T] =
      Mono.create[T] { sink =>
        hungOperations.add(sink)
        sink.onDispose(() => hungOperations.remove(sink))
        if (closed.get() && hungOperations.remove(sink))
          sink.error(connectionClosedException())
      }

    override def beginTransaction(): Publisher[Void] =
      if (applyFault(BeginFails))
        Mono.error(new R2dbcNonTransientResourceException("Injected fault: begin transaction failed"))
      else if (applyFault(BeginHangs))
        hang[Void]()
      else
        delegate.beginTransaction()

    override def beginTransaction(definition: TransactionDefinition): Publisher[Void] =
      delegate.beginTransaction(definition)

    // fails the hung operations, as the driver fails pending requests when its channel closes
    override def close(): Publisher[Void] =
      Mono.defer { () =>
        closed.set(true)
        var sink = hungOperations.poll()
        while (sink != null) {
          sink.error(connectionClosedException())
          sink = hungOperations.poll()
        }
        Mono.from(delegate.close())
      }

    override def commitTransaction(): Publisher[Void] = delegate.commitTransaction()
    override def createBatch(): Batch = delegate.createBatch()
    override def createSavepoint(name: String): Publisher[Void] = delegate.createSavepoint(name)
    override def createStatement(sql: String): Statement =
      if (sql == validationQuery && applyFault(ValidationHangs))
        new HangingStatement(() => hang[Result]())
      else if (sql != validationQuery && applyFault(QueryHangs))
        new HangingStatement(() => hang[Result]())
      else
        delegate.createStatement(sql)
    override def isAutoCommit: Boolean = delegate.isAutoCommit
    override def getMetadata: ConnectionMetadata = delegate.getMetadata
    override def getTransactionIsolationLevel: IsolationLevel = delegate.getTransactionIsolationLevel
    override def releaseSavepoint(name: String): Publisher[Void] = delegate.releaseSavepoint(name)
    override def rollbackTransaction(): Publisher[Void] = delegate.rollbackTransaction()
    override def rollbackTransactionToSavepoint(name: String): Publisher[Void] =
      delegate.rollbackTransactionToSavepoint(name)
    override def setAutoCommit(autoCommit: Boolean): Publisher[Void] =
      if (applyFault(AutoCommitFails))
        Mono.error(new R2dbcNonTransientResourceException("Injected fault: set auto-commit failed"))
      else
        delegate.setAutoCommit(autoCommit)
    override def setLockWaitTimeout(timeout: Duration): Publisher[Void] = delegate.setLockWaitTimeout(timeout)
    override def setStatementTimeout(timeout: Duration): Publisher[Void] = delegate.setStatementTimeout(timeout)
    override def setTransactionIsolationLevel(isolationLevel: IsolationLevel): Publisher[Void] =
      delegate.setTransactionIsolationLevel(isolationLevel)
    override def validate(depth: ValidationDepth): Publisher[java.lang.Boolean] = delegate.validate(depth)
  }

  final class HangingStatement(execution: () => Publisher[Result]) extends Statement {
    override def add(): Statement = this
    override def bind(index: Int, value: Any): Statement = this
    override def bind(name: String, value: Any): Statement = this
    override def bindNull(index: Int, `type`: Class[_]): Statement = this
    override def bindNull(name: String, `type`: Class[_]): Statement = this
    override def execute(): Publisher[_ <: Result] = execution()
  }
}

class ConnectionFailureSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ConnectionFailureSpec._
  import ConnectionFailureSpec.Fault._

  private val connectionFactorySettings =
    ConnectionFactoryProvider(system).connectionFactorySettingsFor("akka.persistence.r2dbc.connection-factory")

  private val closeCallsExceeding = 2.seconds
  // longer than closeCallsExceeding, so that a test can tell which limit ended a call
  private val closeQueriesExceeding = 3.seconds
  // longer than closeQueriesExceeding, so that a test can tell which limit ended a bucket count
  private val estimateTimeRangeTimeout = 4.seconds

  private final class Setup(poolConfig: String) {
    val poolSettings = new ConnectionPoolSettings(
      ConfigFactory
        .parseString(s"""
          initial-size = 0
          max-size = 1
          acquire-timeout = 2 seconds
          acquire-retry = 1
          close-calls-exceeding = ${closeCallsExceeding.toMillis} ms
          close-queries-exceeding = ${closeQueriesExceeding.toMillis} ms
          $poolConfig
          """)
        .withFallback(connectionFactorySettings.config))

    private val optionsProvider: ConnectionFactoryOptionsProvider = (builder, _) => builder.build()

    val connectionFactory = new FaultyConnectionFactory(
      connectionFactorySettings.dialect.createConnectionFactory(connectionFactorySettings.config, optionsProvider),
      poolSettings.validationQuery)

    val pool: ConnectionPool =
      ConnectionFactoryProvider(system).createConnectionPoolFactory(poolSettings, connectionFactory)

    private val log = LoggerFactory.getLogger(classOf[ConnectionFailureSpec])

    val executor = new R2dbcExecutor(
      pool,
      log,
      logDbCallsExceeding = 5.seconds,
      poolSettings.closeCallsExceeding,
      poolSettings.closeQueriesExceeding)(system.executionContext, system)

    // the DAOs of the dialect over this executor
    val executorProvider: R2dbcExecutorProvider = {
      val settings = R2dbcSettings(
        ConfigFactory
          .parseString(s"query.estimate-time-range-timeout = ${estimateTimeRangeTimeout.toMillis} ms")
          .withFallback(system.settings.config.getConfig("akka.persistence.r2dbc")))
      new R2dbcExecutorProvider(
        system,
        system.executionContext,
        settings,
        "akka.persistence.r2dbc.connection-factory",
        log) {
        override def executorFor(slice: Int): R2dbcExecutor = executor
      }
    }

    def selectOne(): Future[Option[Integer]] =
      executor.selectOne("test")(_.createStatement("select 1"), _.get(0, classOf[Integer]))

    def selectOneWithAutoCommit(): Future[Option[Integer]] =
      executor.withAutoCommitConnection("test") { connection =>
        R2dbcExecutor.selectOneInTx(connection.createStatement("select 1"), _.get(0, classOf[Integer]))(
          system.executionContext,
          system)
      }

    def selectOneInTransaction(): Future[Option[Integer]] =
      executor.withConnection("test") { connection =>
        R2dbcExecutor.selectOneInTx(connection.createStatement("select 1"), _.get(0, classOf[Integer]))(
          system.executionContext,
          system)
      }
  }

  private def withPool(poolConfig: String = "")(test: Setup => Unit): Unit = {
    val setup = new Setup(poolConfig)
    try {
      test(setup)
      eventually {
        setup.pool.getMetrics.get.acquiredSize() shouldBe 0
      }
    } finally {
      setup.pool.disposeLater().asFutureDone().futureValue
    }
  }

  "R2dbcExecutor with a failing connection" should {

    "return the connection when the transaction start fails" in withPool() { setup =>
      setup.connectionFactory.plan(BeginFails)

      setup.selectOneInTransaction().failed.futureValue shouldBe a[R2dbcNonTransientResourceException]

      setup.selectOneInTransaction().futureValue shouldBe Some(1)
    }

    "close the connection when the transaction start does not answer" in withPool() { setup =>
      setup.connectionFactory.plan(BeginHangs)

      val startTime = System.nanoTime()
      val result = setup.selectOneInTransaction()
      // after about close-calls-exceeding, not after the patience of the test
      Await.result(result.failed, closeCallsExceeding + 3.seconds) shouldBe a[R2dbcNonTransientResourceException]
      (System.nanoTime() - startTime).nanos should be >= closeCallsExceeding

      setup.selectOneInTransaction().futureValue shouldBe Some(1)
    }

    "return the connection when setting auto-commit fails" in withPool() { setup =>
      setup.connectionFactory.plan(AutoCommitFails)

      setup.selectOneWithAutoCommit().failed.futureValue shouldBe a[R2dbcNonTransientResourceException]

      setup.selectOneWithAutoCommit().futureValue shouldBe Some(1)
    }

    "close the connection when a query does not answer" in withPool() { setup =>
      setup.connectionFactory.plan(QueryHangs)

      val startTime = System.nanoTime()
      val result = setup.selectOne()
      // after about close-queries-exceeding, not after the patience of the test
      Await.result(result.failed, closeQueriesExceeding + 3.seconds) shouldBe a[R2dbcNonTransientResourceException]
      (System.nanoTime() - startTime).nanos should be >= closeQueriesExceeding

      setup.selectOne().futureValue shouldBe Some(1)
    }

    "limit the bucket count of events with estimate-time-range-timeout" in withPool() { setup =>
      setup.connectionFactory.plan(QueryHangs)
      val dao = connectionFactorySettings.dialect.createQueryDao(setup.executorProvider)

      val startTime = System.nanoTime()
      val result = dao.countBuckets("TestEntity", 0, 1023, Instant.EPOCH, Instant.now(), 10, None)
      Await.result(result.failed, estimateTimeRangeTimeout + 3.seconds) shouldBe a[R2dbcNonTransientResourceException]
      (System.nanoTime() - startTime).nanos should be >= estimateTimeRangeTimeout
    }

    "limit the bucket count of durable state with estimate-time-range-timeout" in withPool() { setup =>
      setup.connectionFactory.plan(QueryHangs)
      val dao = connectionFactorySettings.dialect.createDurableStateDao(setup.executorProvider)

      val startTime = System.nanoTime()
      val result = dao.countBuckets("TestEntity", 0, 1023, Instant.EPOCH, Instant.now(), 10, None)
      Await.result(result.failed, estimateTimeRangeTimeout + 3.seconds) shouldBe a[R2dbcNonTransientResourceException]
      (System.nanoTime() - startTime).nanos should be >= estimateTimeRangeTimeout
    }

    "fail a connection creation that does not complete" in withPool("create-connection-timeout = 1 second") { setup =>
      setup.connectionFactory.plan(CreateHangs)

      // through the acquire retry
      setup.selectOne().futureValue shouldBe Some(1)
    }

    "discard a connection whose validation does not complete" in withPool("""
      validation-query = "select 1"
      validation-timeout = 1 second
      """) { setup =>
      setup.connectionFactory.plan(ValidationHangs)

      setup.selectOne().futureValue shouldBe Some(1)
      setup.selectOne().futureValue shouldBe Some(1)

      val connections = setup.connectionFactory.createdConnections
      connections.size shouldBe 2
      eventually {
        connections.head.isClosed shouldBe true
      }
    }
  }

  "ConnectionFactoryProvider" should {
    "warn when validation-timeout is not less than acquire-timeout" in {
      LoggingTestKit.warn("validation-timeout").expect {
        withPool("""
          validation-query = "select 1"
          validation-timeout = 2 seconds
          """) { setup =>
          setup.selectOne().futureValue shouldBe Some(1)
        }
      }
    }
  }
}
