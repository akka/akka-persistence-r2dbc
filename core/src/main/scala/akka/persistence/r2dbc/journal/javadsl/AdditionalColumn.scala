/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.journal.javadsl

import java.util.Optional

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

@ApiMayChange
object AdditionalColumn {

  /**
   * Passed to the [[AdditionalColumn.bind]] method for each event that is about to be written.
   */
  final case class Insert[A](
      persistenceId: String,
      entityType: String,
      slice: Int,
      seqNr: Long,
      value: A,
      metadata: Optional[AnyRef],
      tags: java.util.Set[String])

  sealed trait Binding[+B]

  def bindValue[B](value: B): Binding[B] = new BindValue(value)

  def bindNull[B]: Binding[B] = BindNull

  def skip[B]: Binding[B] = Skip

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final class BindValue[B](val value: B) extends Binding[B]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object BindNull extends Binding[Nothing]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object Skip extends Binding[Nothing]

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
abstract class AdditionalColumn[A, B] {

  def fieldClass: Class[B]

  def columnName: String

  def bind(insert: AdditionalColumn.Insert[A]): AdditionalColumn.Binding[B]

}
