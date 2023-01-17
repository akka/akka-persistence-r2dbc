/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.javadsl

import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.concurrent.ExecutionContext
import akka.Done
import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.DurableStateStorePagedPersistenceIdsQuery
import akka.persistence.query.typed.javadsl.DurableStateStoreBySliceQuery
import akka.persistence.r2dbc.state.scaladsl.{ R2dbcDurableStateStore => ScalaR2dbcDurableStateStore }
import akka.persistence.state.javadsl.DurableStateUpdateStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.stream.javadsl.Source

import scala.compat.java8.FutureConverters.FutureOps

object R2dbcDurableStateStore {
  val Identifier: String = ScalaR2dbcDurableStateStore.Identifier
}

class R2dbcDurableStateStore[A](scalaStore: ScalaR2dbcDurableStateStore[A])(implicit ec: ExecutionContext)
    extends DurableStateUpdateStore[A]
    with DurableStateStoreBySliceQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A] {

  override def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    scalaStore
      .getObject(persistenceId)
      .map(x => GetObjectResult(Optional.ofNullable(x.value.getOrElse(null.asInstanceOf[A])), x.revision))
      .toJava

  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): CompletionStage[Done] =
    scalaStore.upsertObject(persistenceId, revision, value, tag).toJava

  @deprecated(message = "Use the deleteObject overload with revision instead.", since = "1.0.0")
  override def deleteObject(persistenceId: String): CompletionStage[Done] =
    deleteObject(persistenceId, revision = 0)

  override def deleteObject(persistenceId: String, revision: Long): CompletionStage[Done] =
    scalaStore.deleteObject(persistenceId, revision).toJava

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

  /**
   * Get the current persistence ids.
   *
   * Note: to reuse existing index, the actual query filters entity types based on persistence_id column and sql LIKE
   * operator. Hence the persistenceId must start with an entity type followed by default separator ("|") from
   * [[akka.persistence.typed.PersistenceId]].
   *
   * @param entityType
   *   The entity type name.
   * @param afterId
   *   The ID to start returning results from, or empty to return all ids. This should be an id returned from a previous
   *   invocation of this command. Callers should not assume that ids are returned in sorted order.
   * @param limit
   *   The maximum results to return. Use Long.MAX_VALUE to return all results. Must be greater than zero.
   * @return
   *   A source containing all the persistence ids, limited as specified.
   */
  def currentPersistenceIds(entityType: String, afterId: Optional[String], limit: Long): Source[String, NotUsed] = {
    import scala.compat.java8.OptionConverters._
    scalaStore.currentPersistenceIds(entityType, afterId.asScala, limit).asJava
  }

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] = {
    import scala.compat.java8.OptionConverters._
    scalaStore.currentPersistenceIds(afterId.asScala, limit).asJava
  }

  def currentPersistenceIds(): Source[String, NotUsed] =
    scalaStore.currentPersistenceIds().asJava

}
