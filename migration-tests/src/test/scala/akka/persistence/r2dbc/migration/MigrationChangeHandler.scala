/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.r2dbc.state.scaladsl.ChangeHandler

import scala.annotation.nowarn

class MigrationChangeHandler(system: ActorSystem[_]) extends ChangeHandler[Any] {

  private val incrementSql =
    "INSERT INTO test_counter (persistence_id, slice, counter) VALUES ($1, $2, 1) " +
    "ON CONFLICT (persistence_id, slice) DO UPDATE SET counter = excluded.counter + 1"

  private implicit val ec: ExecutionContext = system.executionContext

  println(incrementSql)

  @nowarn("msg=exhaustive")
  override def process(session: R2dbcSession, change: DurableStateChange[Any]): Future[Done] = {
    println("process changer")
    change match {
      case state: UpdatedDurableState[_] =>
        if (state.value.asInstanceOf[String] == "s-handler") {
          val slice = Persistence(system).sliceForPersistenceId(state.persistenceId)
          val stmt = session
            .createStatement(incrementSql)
            .bind(0, state.persistenceId)
            .bind(1, slice)
          session.updateOne(stmt).map(_ => Done)
        } else Future.successful(Done)
    }
  }
}
