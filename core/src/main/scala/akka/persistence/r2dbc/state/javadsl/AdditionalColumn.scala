/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.javadsl

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

@ApiMayChange
object AdditionalColumn {
  final case class Upsert[A](persistenceId: String, entityType: String, slice: Int, revision: Long, value: A)

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
 * @tparam A
 *   The type of the durable state
 * @tparam B
 *   The type of the field stored in the additional column.
 */
@ApiMayChange
abstract class AdditionalColumn[A, B] {

  def fieldClass: Class[B]

  def columnName: String

  def bind(upsert: AdditionalColumn.Upsert[A]): AdditionalColumn.Binding[B]

}
