/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.state.DurableStateStoreAdditionalColumnSpec
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.GetObjectResult
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object H2AdditionalInitForSchemaSpec {

  val conf = ConfigFactory
    .parseString(s"""
      akka.persistence.r2dbc.state {
        additional-columns {
           "CustomEntity" = ["${classOf[DurableStateStoreAdditionalColumnSpec.Column1].getName}"]
         }
      }
      // #additionalColumn
      akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
      akka.persistence.r2dbc.connection-factory {
        protocol = "mem"
        database = "h2-test-db"
        additional-init = "alter table durable_state add if not exists col1 varchar(256)"
      }
      // #additionalColumn
      """)
    .withFallback(ConfigFactory.load())
    .resolve()

}

class H2AdditionalInitForSchemaSpec
    extends ScalaTestWithActorTestKit(H2AdditionalInitForSchemaSpec.conf)
    with TestDbLifecycle
    with TestData
    with AnyWordSpecLike {

  override def typedSystem: ActorSystem[_] = system

  private val store = DurableStateStoreRegistry(testKit.system)
    .durableStateStoreFor[R2dbcDurableStateStore[String]](R2dbcDurableStateStore.Identifier)

  private def exists(whereCondition: String): Boolean =
    r2dbcExecutor
      .selectOne("count")(
        _.createStatement(s"select count(*) from durable_state where $whereCondition"),
        row => row.get(0, classOf[java.lang.Long]).longValue())
      .futureValue
      .contains(1)

  private def existsMatchingCol1(persistenceId: String, columnValue: String): Boolean =
    exists(s"persistence_id = '$persistenceId' and col1 = '$columnValue'")

  "The R2DBC durable state store" should {
    "save and retrieve a value in custom table with additional columns" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"

      store.upsertObject(persistenceId, 1L, value, "n/a").futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      existsMatchingCol1(persistenceId, value) should be(true)
    }
  }

}
