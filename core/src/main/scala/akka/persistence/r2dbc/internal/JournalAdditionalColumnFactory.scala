/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.util.Optional

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.Try

import akka.actor.{ ActorSystem => ClassicActorSystem }
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.journal.javadsl

/**
 * INTERNAL API
 */
@InternalApi private[akka] object JournalAdditionalColumnFactory {

  /**
   * Adapter from javadsl.AdditionalColumn to scaladsl.AdditionalColumn
   */
  final class AdditionalColumnAdapter(delegate: javadsl.AdditionalColumn[Any, Any]) extends AdditionalColumn[Any, Any] {

    override private[akka] val fieldClass: Class[_] =
      delegate.fieldClass

    override def columnName: String =
      delegate.columnName

    override def bind(insert: AdditionalColumn.Insert[Any]): AdditionalColumn.Binding[Any] = {
      val javadslInsert = new javadsl.AdditionalColumn.Insert[Any](
        insert.persistenceId,
        insert.entityType,
        insert.slice,
        insert.seqNr,
        insert.value,
        insert.metadata.map(_.asInstanceOf[AnyRef]).toJava: Optional[AnyRef],
        insert.tags.asJava)
      delegate.bind(javadslInsert) match {
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
      new AdditionalColumnAdapter(javadslColumn)

    tryCreateScaladslInstance()
      .orElse(tryCreateJavadslInstance().map(adapt))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Journal additional column [$fqcn] must implement " +
          s"[${classOf[AdditionalColumn[_, _]].getName}] or [${classOf[javadsl.AdditionalColumn[_, _]].getName}]. It " +
          s"may have an ActorSystem constructor parameter."))
  }

}
