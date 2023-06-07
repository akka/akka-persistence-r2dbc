/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.state

// #change-handler
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.r2dbc.state.scaladsl.ChangeHandler

// #change-handler

/* config:
// #change-handler-config
akka.persistence.r2dbc.state {
  change-handler {
    "BlogPost" = "docs.BlogPostCounts"
  }
}
// #change-handler-config
 */

// #change-handler
/**
 * Keep track of number of published blog posts. Count per slice.
 *
 * {{{
 * CREATE TABLE post_count (slice INT NOT NULL, cnt BIGINT NOT NULL, PRIMARY KEY(slice));
 * }}}
 */
class BlogPostCounts(system: ActorSystem[_]) extends ChangeHandler[BlogPost.State] {

  private val incrementSql =
    "INSERT INTO post_count (slice, cnt) VALUES ($1, 1) " +
    "ON CONFLICT (slice) DO UPDATE SET cnt = excluded.cnt + 1"

  private val decrementSql =
    "UPDATE post_count SET cnt = cnt - 1 WHERE slice = $1"

  private implicit val ec: ExecutionContext = system.executionContext

  override def process(session: R2dbcSession, change: DurableStateChange[BlogPost.State]): Future[Done] = {
    change match {
      case upd: UpdatedDurableState[BlogPost.State] =>
        upd.value match {
          case _: BlogPost.PublishedState =>
            val slice = Persistence(system).sliceForPersistenceId(upd.persistenceId)
            val stmt = session
              .createStatement(incrementSql)
              .bind(0, slice)
            session.updateOne(stmt).map(_ => Done)
          case _ =>
            Future.successful(Done)
        }

      case del: DeletedDurableState[BlogPost.State] =>
        val slice = Persistence(system).sliceForPersistenceId(del.persistenceId)
        val stmt = session
          .createStatement(decrementSql)
          .bind(0, slice)
        session.updateOne(stmt).map(_ => Done)
    }
  }
}
// #change-handler
