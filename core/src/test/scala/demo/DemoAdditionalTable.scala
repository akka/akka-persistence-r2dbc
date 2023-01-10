/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package demo

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.state.scaladsl.DaoExtension
import akka.persistence.r2dbc.state.scaladsl.R2dbcSession
import akka.persistence.r2dbc.internal.Sql.Interpolation

object DemoAdditionalTable {

  /**
   * Keep track of number of published blog posts. Count per slice.
   *
   * {{{
   * CREATE TABLE post_count (slice INT NOT NULL, cnt BIGINT NOT NULL, PRIMARY KEY(slice));
   * }}}
   */
  class CustomDurableState(system: ActorSystem[_]) extends DaoExtension {
    val incrementSql = sql"""
        INSERT INTO post_count
        (slice, cnt)
        VALUES (?, 1)
        ON CONFLICT (slice)
        DO UPDATE SET
          cnt = excluded.cnt + 1
        """

    val decrementSql = sql"""
        UPDATE post_count SET cnt = cnt - 1 WHERE slice = ?;
        """

    private implicit val ec: ExecutionContext = system.executionContext

    override def processOnUpsert(upsert: DaoExtension.Upsert[AnyRef], session: R2dbcSession): Future[Done] = {
      if (upsert.entityType == BlogPost.EntityTypeName) {
        upsert.value match {
          case _: BlogPost.PublishedState =>
            val stmt = session
              .createStatement(incrementSql)
              .bind(0, upsert.slice)
            session.updateOne(stmt).map(_ => Done)
          case _ =>
            Future.successful(Done)
        }
      } else {
        super.processOnUpsert(upsert, session)
      }
    }

    override def processOnDelete(delete: DaoExtension.Delete, session: R2dbcSession): Future[Done] = {
      if (delete.entityType == BlogPost.EntityTypeName) {
        val stmt = session
          .createStatement(decrementSql)
          .bind(0, delete.slice)
        session.updateOne(stmt).map(_ => Done)
      } else {
        super.processOnDelete(delete, session)
      }
    }
  }

}
