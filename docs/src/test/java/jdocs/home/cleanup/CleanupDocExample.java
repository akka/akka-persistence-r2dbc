/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home.cleanup;

import akka.actor.typed.ActorSystem;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.CurrentPersistenceIdsQuery;
import akka.persistence.r2dbc.cleanup.scaladsl.EventSourcedCleanup;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import scala.jdk.javaapi.FutureConverters;

public class CleanupDocExample {

  public static void example() {

    ActorSystem<?> system = null;

    // #cleanup
    CurrentPersistenceIdsQuery queries =
        PersistenceQuery.get(system)
            .getReadJournalFor(CurrentPersistenceIdsQuery.class, R2dbcReadJournal.Identifier());
    EventSourcedCleanup cleanup = new EventSourcedCleanup(system);

    int persistenceIdParallelism = 10;

    // forall persistence ids, delete all events before the snapshot
    queries
        .currentPersistenceIds()
        .mapAsync(
            persistenceIdParallelism,
            pid -> FutureConverters.asJava(cleanup.cleanupBeforeSnapshot(pid)))
        .run(system);

    // #cleanup

  }
}
