/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.home.journal;

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
      "SELECT persistence_id, seq_nr, event_ser_id, event_ser_manifest, event_payload "
          + "FROM event_journal_blogpost "
          + "WHERE title = $1 AND deleted = false "
          + "ORDER BY persistence_id, seq_nr";

  public CompletionStage<List<BlogPost.Event>> findByTitle(String title) {
    return R2dbcSession.withSession(
        system,
        session -> {
          Statement stmt = session.createStatement(findByTitleSql).bind(0, title);
          return session.select(
              stmt,
              row -> {
                int serializerId = row.get("event_ser_id", Integer.class);
                String serializerManifest = row.get("event_ser_manifest", String.class);
                byte[] payload = row.get("event_payload", byte[].class);
                BlogPost.Event event =
                    (BlogPost.Event)
                        SerializationExtension.get(system)
                            .deserialize(payload, serializerId, serializerManifest)
                            .get();
                return event;
              });
        });
  }
}
// #query
