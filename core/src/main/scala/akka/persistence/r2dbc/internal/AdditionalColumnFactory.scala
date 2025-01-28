/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.util.Try

import akka.actor.{ ActorSystem => ClassicActorSystem }
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.state.javadsl

/**
 * INTERNAL API
 */
@InternalApi private[akka] object AdditionalColumnFactory {

  /**
   * Adapter from javadsl.AdditionColumn to scaladsl.AdditionalColumn
   */
  final class AdditionColumnAdapter(delegate: javadsl.AdditionalColumn[Any, Any]) extends AdditionalColumn[Any, Any] {

    override private[akka] val fieldClass: Class[_] =
      delegate.fieldClass

    override def columnName: String =
      delegate.columnName

    override def bind(upsert: AdditionalColumn.Upsert[Any]): AdditionalColumn.Binding[Any] = {
      val javadslUpsert = new javadsl.AdditionalColumn.Upsert[Any](
        upsert.persistenceId,
        upsert.entityType,
        upsert.slice,
        upsert.revision,
        upsert.value)
      delegate.bind(javadslUpsert) match {
        case bindValue: javadsl.AdditionalColumn.BindValue[_] => AdditionalColumn.BindValue(bindValue.value)
        case javadsl.AdditionalColumn.BindNull                => AdditionalColumn.BindNull
        case javadsl.AdditionalColumn.Skip                    => AdditionalColumn.Skip
      }
    }

  }

  def create(system: ActorSystem[_], fqcn: String): AdditionalColumn[Any, Any] = {
    val dynamicAccess = system.classicSystem.asInstanceOf[ExtendedActorSystem].dynamicAccess

    def tryCreateScaladslInstance(): Try[AdditionalColumn[Any, Any]] = {
      dynamicAccess
        .createInstanceFor[AdditionalColumn[Any, Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[AdditionalColumn[Any, Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(dynamicAccess.createInstanceFor[AdditionalColumn[Any, Any]](
              fqcn,
              List(classOf[ClassicActorSystem] -> system.classicSystem))))
    }

    def tryCreateJavadslInstance(): Try[javadsl.AdditionalColumn[Any, Any]] = {
      dynamicAccess
        .createInstanceFor[javadsl.AdditionalColumn[Any, Any]](fqcn, Nil)
        .orElse(
          dynamicAccess
            .createInstanceFor[javadsl.AdditionalColumn[Any, Any]](fqcn, List(classOf[ActorSystem[_]] -> system))
            .orElse(dynamicAccess.createInstanceFor[javadsl.AdditionalColumn[Any, Any]](
              fqcn,
              List(classOf[ClassicActorSystem] -> system.classicSystem))))
    }

    def adapt(javadslColumn: javadsl.AdditionalColumn[Any, Any]): AdditionalColumn[Any, Any] =
      new AdditionColumnAdapter(javadslColumn)

    tryCreateScaladslInstance()
      .orElse(tryCreateJavadslInstance().map(adapt))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Additional column [$fqcn] must implement " +
          s"[${classOf[AdditionalColumn[_, _]].getName}] or [${classOf[javadsl.AdditionalColumn[_, _]].getName}]. It " +
          s"may have an ActorSystem constructor parameter."))
  }

}
