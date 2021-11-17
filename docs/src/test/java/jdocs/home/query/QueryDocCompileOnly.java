package jdocs.home.query;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.japi.Pair;
import akka.persistence.query.NoOffset;
import akka.persistence.typed.PersistenceId;
import akka.stream.javadsl.Sink;
import java.util.List;

//#readJournalFor
import akka.persistence.query.PersistenceQuery;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;

//#readJournalFor

//#durableStateStoreFor
import akka.persistence.r2dbc.state.javadsl.R2dbcDurableStateStore;
import akka.persistence.state.DurableStateStoreRegistry;

//#durableStateStoreFor

//#currentEventsBySlices
import akka.stream.javadsl.Source;
import akka.persistence.query.typed.EventEnvelope;

//#currentEventsBySlices

//#currentChangesBySlices
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.UpdatedDurableState;

//#currentChangesBySlices

public class QueryDocCompileOnly {

    interface MyEvent{}
    interface MyState{}

    private final ActorSystem<?> system = ActorSystem.create(Behaviors.empty(), "Docs");

    //#readJournalFor
    R2dbcReadJournal eventQueries = PersistenceQuery.get(system)
        .getReadJournalFor(R2dbcReadJournal.class, R2dbcReadJournal.Identifier());
    //##readJournalFor

    //#durableStateStoreFor
    R2dbcDurableStateStore<MyState> stateQueries = DurableStateStoreRegistry.get(system)
        .getDurableStateStoreFor(R2dbcDurableStateStore.class, R2dbcDurableStateStore.Identifier());
    //#durableStateStoreFor

    void exampleEventsByPid() {
        //#currentEventsByPersistenceId
        PersistenceId persistenceId = PersistenceId.of("MyEntity", "id1");
        eventQueries
            .currentEventsByPersistenceId(persistenceId.id(), 1, 101)
            .map(envelope -> "event with seqNr " + envelope.sequenceNr() + ": " + envelope.event())
            .runWith(Sink.foreach(System.out::println), system);
        //#currentEventsByPersistenceId
    }

    void exampleEventsBySlices() {
        //#currentEventsBySlices
        // Slit the slices into 4 ranges
        int numberOfSliceRanges = 4;
        List<Pair<Integer, Integer>> sliceRanges = eventQueries.sliceRanges(numberOfSliceRanges);

        // Example of using the first slice range
        int minSlice = sliceRanges.get(0).first();
        int maxSlice = sliceRanges.get(0).second();
        String entityType = "MyEntity";
        Source<EventEnvelope<MyEvent>, NotUsed> source =
            eventQueries.currentEventsBySlices(entityType, minSlice, maxSlice, NoOffset.getInstance());
        source
            .map(envelope -> "event from persistenceId " + envelope.persistenceId() +
                " with seqNr " + envelope.sequenceNr() + ": " + envelope.event())
            .runWith(Sink.foreach(System.out::println), system);
        //#currentEventsBySlices
    }

    void exampleStateBySlices() {
        //#currentChangesBySlices
        // Slit the slices into 4 ranges
        int numberOfSliceRanges = 4;
        List<Pair<Integer, Integer>> sliceRanges = stateQueries.sliceRanges(numberOfSliceRanges);

        // Example of using the first slice range
        int minSlice = sliceRanges.get(0).first();
        int maxSlice = sliceRanges.get(0).second();
        String entityType = "MyEntity";
        Source<DurableStateChange<MyState>, NotUsed> source =
            stateQueries.currentChangesBySlices(entityType, minSlice, maxSlice, NoOffset.getInstance());
        source
            .collectType(UpdatedDurableState.class)
            .map(change -> "state change from persistenceId " + change.persistenceId() +
                " with revision " + change.revision() + ": " + change.value())
            .runWith(Sink.foreach(System.out::println), system);
        //#currentChangesBySlices
    }
}
