/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.session.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.session.scaladsl
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

@ApiMayChange
object R2dbcSession {

  /**
   * Runs the passed function in using a R2dbcSession with a new transaction. The connection is closed and the
   * transaction is committed at the end or rolled back in case of failures.
   */
  def withSession[A](system: ActorSystem[_], fun: JFunction[R2dbcSession, CompletionStage[A]]): CompletionStage[A] = {
    withSession(system, s"akka.persistence.r2dbc.connection-factory", fun)
  }

  def withSession[A](
      system: ActorSystem[_],
      connectionFactoryConfigPath: String,
      fun: JFunction[R2dbcSession, CompletionStage[A]]): CompletionStage[A] = {
    scaladsl.R2dbcSession.withSession(system, connectionFactoryConfigPath) { scaladslSession =>
      val javadslSession = new R2dbcSession(scaladslSession.connection)(system.executionContext, system)
      fun(javadslSession).toScala
    }
  }.toJava

}

@ApiMayChange
final class R2dbcSession(val connection: Connection)(implicit ec: ExecutionContext, system: ActorSystem[_]) {

  def createStatement(sql: String): Statement =
    connection.createStatement(sql)

  def updateOne(statement: Statement): CompletionStage[java.lang.Long] =
    R2dbcExecutor.updateOneInTx(statement).map(java.lang.Long.valueOf)(ExecutionContexts.parasitic).toJava

  def update(statements: java.util.List[Statement]): CompletionStage[java.util.List[java.lang.Long]] =
    R2dbcExecutor
      .updateInTx(statements.asScala.toVector)
      .map(results => results.map(java.lang.Long.valueOf).asJava)
      .toJava

  def selectOne[A](statement: Statement)(mapRow: JFunction[Row, A]): CompletionStage[Optional[A]] =
    R2dbcExecutor.selectOneInTx(statement, mapRow(_)).map(_.asJava)(ExecutionContexts.parasitic).toJava

  def select[A](statement: Statement)(mapRow: JFunction[Row, A]): CompletionStage[java.util.List[A]] =
    R2dbcExecutor.selectInTx(statement, mapRow(_)).map(_.asJava).toJava

}
