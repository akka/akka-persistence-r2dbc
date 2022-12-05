package jdocs.home.cleanup;

import akka.actor.typed.ActorSystem;
import akka.persistence.query.PersistenceQuery;
import akka.persistence.query.javadsl.CurrentPersistenceIdsQuery;
import akka.persistence.r2dbc.cleanup.scaladsl.EventSourcedCleanup;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import scala.compat.java8.FutureConverters;

public class CleanupDocExample {


    public static void example() {

        ActorSystem<?> system = null;

        //#cleanup
        CurrentPersistenceIdsQuery queries = PersistenceQuery.get(system)
            .getReadJournalFor(CurrentPersistenceIdsQuery.class, R2dbcReadJournal.Identifier());
        EventSourcedCleanup cleanup = new EventSourcedCleanup(system);

        int persistenceIdParallelism = 10;


        // forall persistence ids, delete all events before the snapshot
        queries
            .currentPersistenceIds()
            .mapAsync(persistenceIdParallelism, pid ->
                FutureConverters.toJava(cleanup.cleanupBeforeSnapshot(pid)))
            .run(system);

        //#cleanup


    }
}
