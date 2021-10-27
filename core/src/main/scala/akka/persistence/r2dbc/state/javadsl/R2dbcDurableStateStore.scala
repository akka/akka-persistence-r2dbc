/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.r2dbc.state.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.FutureOps

import akka.Done
import akka.persistence.r2dbc.state.scaladsl.{ R2dbcDurableStateStore => ScalaR2dbcDurableStateStore }
import akka.persistence.state.javadsl.DurableStateUpdateStore
import akka.persistence.state.javadsl.GetObjectResult

object R2dbcDurableStateStore {
  val Identifier = ScalaR2dbcDurableStateStore.Identifier
}

class R2dbcDurableStateStore[A](scalaStore: ScalaR2dbcDurableStateStore[A])(implicit ec: ExecutionContext)
    extends DurableStateUpdateStore[A] {

  def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    scalaStore
      .getObject(persistenceId)
      .map(x => GetObjectResult(Optional.ofNullable(x.value.getOrElse(null.asInstanceOf[A])), x.revision))
      .asJava

  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done] =
    scalaStore.upsertObject(persistenceId, revision, value, tag).asJava

  def deleteObject(persistenceId: String): CompletionStage[Done] =
    scalaStore.deleteObject(persistenceId).asJava

}
