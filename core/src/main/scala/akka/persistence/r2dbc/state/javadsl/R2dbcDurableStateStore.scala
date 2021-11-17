/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.javadsl

import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters.FutureOps

import akka.Done
import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.CurrentDurableStatePersistenceIdsQuery
import akka.persistence.query.typed.javadsl.DurableStateStoreBySliceQuery
import akka.persistence.r2dbc.state.scaladsl.{ R2dbcDurableStateStore => ScalaR2dbcDurableStateStore }
import akka.persistence.state.javadsl.DurableStateUpdateStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.stream.javadsl.Source

object R2dbcDurableStateStore {
  val Identifier: String = ScalaR2dbcDurableStateStore.Identifier
}

class R2dbcDurableStateStore[A](scalaStore: ScalaR2dbcDurableStateStore[A])(implicit ec: ExecutionContext)
    extends DurableStateUpdateStore[A]
    with DurableStateStoreBySliceQuery[A]
    with CurrentDurableStatePersistenceIdsQuery[A] {

  def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    scalaStore
      .getObject(persistenceId)
      .map(x => GetObjectResult(Optional.ofNullable(x.value.getOrElse(null.asInstanceOf[A])), x.revision))
      .asJava

  def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done] =
    scalaStore.upsertObject(persistenceId, revision, value, tag).asJava

  def deleteObject(persistenceId: String): CompletionStage[Done] =
    scalaStore.deleteObject(persistenceId).asJava

  override def currentChangesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    scalaStore.currentChangesBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def changesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    scalaStore.changesBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceForPersistenceId(persistenceId: String): Int =
    scalaStore.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import akka.util.ccompat.JavaConverters._
    scalaStore
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] = {
    import scala.compat.java8.OptionConverters._
    scalaStore.currentPersistenceIds(afterId.asScala, limit).asJava
  }
}
