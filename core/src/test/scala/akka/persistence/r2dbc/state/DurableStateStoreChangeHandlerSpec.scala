/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.r2dbc.state.scaladsl.ChangeHandler
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.GetObjectResult
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object DurableStateStoreChangeHandlerSpec {
  val config: Config = ConfigFactory
    .parseString(s"""
    akka.persistence.r2dbc.state {
      change-handler {
        "CustomEntity" = "${classOf[Handler].getName}"
        "JavadslCustomEntity" = "${classOf[JavadslChangeHandler].getName}"
      }
    }
    """)
    .withFallback(TestConfig.config)

  class Handler(system: ActorSystem[_]) extends ChangeHandler[String] {
    private implicit val ec: ExecutionContext = system.executionContext

    override def process(session: R2dbcSession, change: DurableStateChange[String]): Future[Done] = {
      change match {
        case upd: UpdatedDurableState[String] =>
          if (upd.value == "BOOM")
            Future.failed(new RuntimeException("BOOM"))
          else
            session
              .updateOne(
                session
                  .createStatement(sql"insert into changes_test (pid, rev, value) values (?, ?, ?)")
                  .bind(0, upd.persistenceId)
                  .bind(1, upd.revision)
                  .bind(2, upd.value))
              .map(_ => Done)

        case del: DeletedDurableState[String] =>
          session
            .updateOne(
              session
                .createStatement(sql"insert into changes_test (pid, rev, value) values (?, ?, ?)")
                .bind(0, del.persistenceId)
                .bind(1, del.revision)
                .bindNull(2, classOf[String]))
            .map(_ => Done)
      }
    }
  }

}

class DurableStateStoreChangeHandlerSpec
    extends ScalaTestWithActorTestKit(DurableStateStoreChangeHandlerSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  private val anotherTable = "changes_test"

  override def typedSystem: ActorSystem[_] = system

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(
      r2dbcExecutor.executeDdl("beforeAll create durable_state_test")(
        _.createStatement(
          s"create table if not exists $anotherTable (pid varchar(256), rev bigint, value varchar(256))")),
      20.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from $anotherTable")),
      10.seconds)
  }

  private val store = DurableStateStoreRegistry(testKit.system)
    .durableStateStoreFor[R2dbcDurableStateStore[String]](R2dbcDurableStateStore.Identifier)

  private val unusedTag = "n/a"

  private def exists(whereCondition: String): Boolean =
    r2dbcExecutor
      .selectOne("count")(
        _.createStatement(s"select count(*) from $anotherTable where $whereCondition"),
        row => row.get(0, classOf[java.lang.Long]).longValue())
      .futureValue
      .contains(1)

  "The R2DBC durable state store change handler" should {
    "be invoked for first revision" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"

      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      exists(s"pid = '$persistenceId' and rev = 1 and value = '$value'") should be(true)
    }

    "be invoked for updates" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "Open to Feedback"
      store.upsertObject(persistenceId, 2L, updatedValue, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(updatedValue), 2L))

      exists(s"pid = '$persistenceId' and rev = 2 and value = '$updatedValue'") should be(true)
    }

    "be invoked for deletes" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      store.deleteObject(persistenceId, 2L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 2L))

      exists(s"pid = '$persistenceId' and rev = 2 and value is null") should be(true)
    }

    "be invoked for hard deletes" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      // revision 0 is for hard delete
      store.deleteObject(persistenceId, 0L).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(None, 0L))

      exists(s"pid = '$persistenceId' and rev = 0 and value is null") should be(true)
    }

    "use same transaction" in {
      val entityType = "CustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Genuinely Collaborative"
      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      val updatedValue = "BOOM"
      store.upsertObject(persistenceId, 2L, updatedValue, unusedTag).failed.futureValue.getMessage should be(
        s"Change handler update failed for [$persistenceId] revision [2], due to BOOM")
      // still old value
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      exists(s"pid = '$persistenceId' and rev = 2") should be(false)
    }

    "support javadsl.ChangeHandler" in {
      val entityType = "JavadslCustomEntity"
      val persistenceId = nextPid(entityType)
      val value = "Run anywhere"

      store.upsertObject(persistenceId, 1L, value, unusedTag).futureValue
      store.getObject(persistenceId).futureValue should be(GetObjectResult(Some(value), 1L))

      exists(s"pid = '$persistenceId' and rev = 1 and value = '$value'") should be(true)
    }

  }

}
