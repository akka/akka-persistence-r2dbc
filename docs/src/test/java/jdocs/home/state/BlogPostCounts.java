/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home.state;

// #change-handler
import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.persistence.Persistence;
import akka.persistence.query.DeletedDurableState;
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.UpdatedDurableState;
import akka.persistence.r2dbc.session.javadsl.R2dbcSession;
import akka.persistence.r2dbc.state.javadsl.ChangeHandler;
import io.r2dbc.spi.Statement;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Keep track of number of published blog posts. Count per slice.
 *
 * <pre>
 * CREATE TABLE post_count (slice INT NOT NULL, cnt BIGINT NOT NULL, PRIMARY KEY(slice));
 * </pre>
 */
public class BlogPostCounts implements ChangeHandler<BlogPost.State> {

  private final ActorSystem<?> system;

  private final String incrementSql =
      "INSERT INTO post_count (slice, cnt) VALUES ($1, 1) " +
          "ON CONFLICT (slice) DO UPDATE SET cnt = excluded.cnt + 1";

  private final String decrementSql =
      "UPDATE post_count SET cnt = cnt - 1 WHERE slice = $1";

  public BlogPostCounts(ActorSystem<?> system) {
    this.system = system;
  }

  @Override
  public CompletionStage<Done> process(R2dbcSession session, DurableStateChange<BlogPost.State> change) {
    if (change instanceof UpdatedDurableState) {
      return processUpdate(session, (UpdatedDurableState<BlogPost.State>) change);
    } else if (change instanceof DeletedDurableState) {
      return processDelete(session, (DeletedDurableState<BlogPost.State>) change);
    } else {
      throw new IllegalArgumentException("Unexpected change " + change.getClass().getName());
    }
  }

  private CompletionStage<Done> processUpdate(R2dbcSession session, UpdatedDurableState<BlogPost.State> upd) {
    if (upd.value() instanceof BlogPost.PublishedState) {
      int slice = Persistence.get(system).sliceForPersistenceId(upd.persistenceId());
      Statement stmt = session
          .createStatement(incrementSql)
          .bind(0, slice);
      return session.updateOne(stmt).thenApply(count -> Done.getInstance());
    } else {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }

  private CompletionStage<Done> processDelete(R2dbcSession session, DeletedDurableState<BlogPost.State> del) {
    int slice = Persistence.get(system).sliceForPersistenceId(del.persistenceId());
    Statement stmt = session
        .createStatement(decrementSql)
        .bind(0, slice);
    return session.updateOne(stmt).thenApply(count -> Done.getInstance());
  }

}
// #change-handler
