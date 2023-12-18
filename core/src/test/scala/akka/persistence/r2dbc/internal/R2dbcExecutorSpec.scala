/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.h2.H2Dialect
import akka.persistence.r2dbc.internal.postgres.PostgresDialect
import akka.persistence.r2dbc.internal.postgres.YugabyteDialect
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.r2dbc.spi.Connection
import io.r2dbc.spi.R2dbcNonTransientResourceException
import io.r2dbc.spi.Wrapped
import org.scalatest.wordspec.AnyWordSpecLike

object R2dbcExecutorSpec {
  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.connection-factory {
      initial-size = 1
      max-size = 1
      close-calls-exceeding = 3 seconds
    }
    """)
    .withFallback(TestConfig.config)
}

class R2dbcExecutorSpec
    extends ScalaTestWithActorTestKit(R2dbcExecutorSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val table = "r2dbc_executor_spec"

  case class Row(col: String)

  // need pg_sleep or similar
  // should we add sqlserver here?
  private def canBeTestedWithDialect: Boolean =
    r2dbcSettings.connectionFactorySettings.dialect == PostgresDialect ||
    r2dbcSettings.connectionFactorySettings.dialect == YugabyteDialect

  private def pendingIfCannotBeTestedWithDialect(): Unit = {
    if (!canBeTestedWithDialect) {
      info(s"Can't be tested with dialect [${r2dbcSettings.dialectName}]")
      pending
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (canBeTestedWithDialect) {
      Await.result(
        r2dbcExecutor.executeDdl(s"beforeAll create table $table")(
          _.createStatement(s"create table if not exists $table (col text)")),
        20.seconds)

      r2dbcExecutor.updateOne("test")(_.createStatement(s"delete from $table")).futureValue
    }
  }

  "R2dbcExecutor" should {

    "close connection when no response from update" in {
      pendingIfCannotBeTestedWithDialect()

      @volatile var c1: Connection = null
      @volatile var c2: Connection = null

      val result = r2dbcExecutor.update("test") { connection =>
        c1 = connection.asInstanceOf[Wrapped[Connection]].unwrap()
        Vector(
          connection.createStatement(s"insert into $table (col) values ('a')"),
          connection.createStatement("select pg_sleep(4)"),
          connection.createStatement(s"insert into $table (col) values ('b')"))
      }

      Thread.sleep(r2dbcSettings.connectionFactorySettings.poolSettings.closeCallsExceeding.get.toMillis)

      // The request will fail with PostgresConnectionClosedException
      result.failed.futureValue shouldBe a[R2dbcNonTransientResourceException]

      r2dbcExecutor
        .selectOne("test")(
          { connection =>
            c2 = connection.asInstanceOf[Wrapped[Connection]].unwrap()
            connection.createStatement(s"select col from $table where col = 'a'")
          },
          row => Row(row.get("col", classOf[String])))
        .futureValue shouldBe None

      r2dbcExecutor
        .selectOne("test")(
          _.createStatement(s"select col from $table where col = 'b'"),
          row => Row(row.get("col", classOf[String])))
        .futureValue shouldBe None

      // it shouldn't reuse the connection
      c1 should not be theSameInstanceAs(c2)
    }

    "close connection when no response from update with auto-commit" in {
      pendingIfCannotBeTestedWithDialect()

      val result = r2dbcExecutor.updateOne("test")(_.createStatement("select pg_sleep(4)"))

      Thread.sleep(r2dbcSettings.connectionFactorySettings.poolSettings.closeCallsExceeding.get.toMillis)

      // The request will fail with PostgresConnectionClosedException
      result.failed.futureValue shouldBe a[R2dbcNonTransientResourceException]
    }

  }
}
