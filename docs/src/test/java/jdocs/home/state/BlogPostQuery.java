/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home.state;

// #query
import akka.actor.typed.ActorSystem;
import akka.persistence.r2dbc.session.javadsl.R2dbcSession;
import akka.serialization.SerializationExtension;
import io.r2dbc.spi.Statement;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class BlogPostQuery {
  private final ActorSystem<?> system;

  public BlogPostQuery(ActorSystem<?> system) {
    this.system = system;
  }

  private final String findByTitleSql =
      "SELECT state_ser_id, state_ser_manifest, state_payload " +
      "FROM durable_state_blog_post " +
      "WHERE title = $1";

  public CompletionStage<List<BlogPost.State>> findByTitle(String title) {
    return R2dbcSession.withSession(system, session -> {
      Statement stmt = session.createStatement(findByTitleSql).bind(0, title);
      return session.select(stmt, row -> {
        int serializerId = row.get("state_ser_id", Integer.class);
        String serializerManifest = row.get("state_ser_manifest", String.class);
        byte[] payload = row.get("state_payload", byte[].class);
        BlogPost.State state = (BlogPost.State) SerializationExtension.get(system)
            .deserialize(payload, serializerId, serializerManifest).get();
        return state;
      });
    });
  }

}
// #query
