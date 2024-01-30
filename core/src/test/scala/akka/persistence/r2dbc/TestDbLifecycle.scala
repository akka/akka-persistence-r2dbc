/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.R2dbcExecutor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.h2.H2Dialect

import java.time.Instant
import scala.util.control.NonFatal

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.persistence.r2dbc"

  lazy val r2dbcSettings: R2dbcSettings =
    R2dbcSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem)
        .connectionFactoryFor(testConfigPath + ".connection-factory"),
      LoggerFactory.getLogger(getClass),
      r2dbcSettings.logDbCallsExceeding,
      r2dbcSettings.connectionFactorySettings.poolSettings.closeCallsExceeding)(
      typedSystem.executionContext,
      typedSystem)
  }

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  override protected def beforeAll(): Unit = {
    try {
      r2dbcSettings.alljournalTablesWithSchema.foreach { table =>
        Await.result(r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from $table")), 10.seconds)
      }
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.snapshotsTableWithSchema}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.durableStateTableWithSchema}")),
        10.seconds)
    } catch {
      case NonFatal(ex) => throw new RuntimeException(s"Test db cleanup failed", ex)
    }
    super.beforeAll()
  }

}
