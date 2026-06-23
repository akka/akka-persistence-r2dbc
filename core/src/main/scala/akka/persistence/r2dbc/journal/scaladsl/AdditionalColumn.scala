/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.journal.scaladsl

import scala.reflect.ClassTag

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

@ApiMayChange
object AdditionalColumn {

  /**
   * Passed to the [[AdditionalColumn.bind]] method for each event that is about to be written.
   *
   * @param persistenceId
   *   The persistence id of the event source actor.
   * @param entityType
   *   The entity type, as extracted from the [[persistenceId]].
   * @param slice
   *   The slice of the [[persistenceId]].
   * @param seqNr
   *   The sequence number of the event.
   * @param value
   *   The deserialized event (after any `Tagged` unwrapping). For events that are persisted in already-serialized form
   *   (such as Replicated Event Sourcing) or migrated with the migration tool, the event is deserialized from the
   *   stored payload before `bind` is called.
   * @param metadata
   *   The optional event metadata, deserialized. Same value as `EventSourcedBehavior.currentMetadata`. May be `None`
   *   for already-serialized events even when metadata was stored.
   * @param tags
   *   The tags of the event.
   */
  final case class Insert[A](
      persistenceId: String,
      entityType: String,
      slice: Int,
      seqNr: Long,
      value: A,
      metadata: Option[Any],
      tags: Set[String])

  sealed trait Binding[+B]

  final case class BindValue[B](value: B) extends Binding[B]

  case object BindNull extends Binding[Nothing]

  case object Skip extends Binding[Nothing]

  private val scalaPrimitivesMapping: Map[Class[_], Class[_]] =
    Map(
      classOf[Int] -> classOf[java.lang.Integer],
      classOf[Long] -> classOf[java.lang.Long],
      classOf[Float] -> classOf[java.lang.Float],
      classOf[Double] -> classOf[java.lang.Double],
      classOf[Byte] -> classOf[java.lang.Byte],
      classOf[Short] -> classOf[java.lang.Short],
      classOf[Char] -> classOf[java.lang.Character])
}

/**
 * Implement this to extract a field from the event and store it in an additional database column on the event journal
 * table for the entity type. The additional column can be used to find events by a secondary index. The intended use is
 * for the application metadata `JSONB` column or similar.
 *
 * Configured per entity type via `akka.persistence.r2dbc.journal.additional-columns`. The implementation may have an
 * `ActorSystem` constructor parameter.
 *
 * The additional column must be `NULL`-able in the database, because delete-marker (tombstone) rows do not invoke
 * `bind`. Inside an `AtomicWrite` batch all events must produce the same shape of bindings (same `Skip` decisions),
 * otherwise the batched insert is rejected.
 *
 * @tparam A
 *   The type of the event.
 * @tparam B
 *   The type of the field stored in the additional column.
 */
@ApiMayChange
abstract class AdditionalColumn[A, B: ClassTag] {
  import AdditionalColumn.scalaPrimitivesMapping

  /**
   * INTERNAL API: used when binding null
   */
  @InternalApi private[akka] val fieldClass: Class[_] = {
    val cls = implicitly[ClassTag[B]].runtimeClass
    scalaPrimitivesMapping.getOrElse(cls, cls)
  }

  def columnName: String

  def bind(insert: AdditionalColumn.Insert[A]): AdditionalColumn.Binding[B]

}
