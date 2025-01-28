/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.annotation.ApiMayChange
import akka.persistence.query.DurableStateChange
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession

@ApiMayChange
trait ChangeHandler[A] {

  /**
   * Implement this method to perform additional processing in the same transaction as the Durable State upsert or
   * delete.
   *
   * The `process` method is invoked for each `DurableStateChange`. Each time a new `Connection` is passed with a new
   * open transaction. You can use `createStatement`, `update` and other methods provided by the [[R2dbcSession]]. The
   * results of several statements can be combined with `CompletionStage` composition (e.g. `thenCompose`). The
   * transaction will be automatically committed or rolled back when the returned `CompletionStage` is completed. Note
   * that an exception here will abort the transaction and fail the upsert or delete.
   *
   * The `ChangeHandler` should be implemented as a stateless function without mutable state because the same
   * `ChangeHandler` instance may be invoked concurrently for different entities. For a specific entity (persistenceId)
   * one change is processed at a time and this `process` method will not be invoked with the next change for that
   * entity until after the returned `Future` is completed.
   */
  def process(session: R2dbcSession, change: DurableStateChange[A]): Future[Done]

}
