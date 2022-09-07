/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.query

import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Sink

object QueryDocCompileOnly {
  implicit val system: ActorSystem[_] = ???
  trait MyEvent
  trait MyState

  //#readJournalFor
  import akka.persistence.query.PersistenceQuery
  import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal

  val eventQueries = PersistenceQuery(system)
    .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
  //#readJournalFor

  //#durableStateStoreFor
  import akka.persistence.state.DurableStateStoreRegistry
  import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore

  val stateQueries = DurableStateStoreRegistry(system)
    .durableStateStoreFor[R2dbcDurableStateStore[MyState]](R2dbcDurableStateStore.Identifier)
  //#durableStateStoreFor

  {
    //#currentEventsByPersistenceId
    val persistenceId = PersistenceId("MyEntity", "id1")
    eventQueries
      .currentEventsByPersistenceId(persistenceId.id, 1, 101)
      .map(envelope => s"event with seqNr ${envelope.sequenceNr}: ${envelope.event}")
      .runWith(Sink.foreach(println))
    //#currentEventsByPersistenceId
  }

  {
    //#currentEventsBySlices
    import akka.persistence.query.typed.EventEnvelope

    // Slit the slices into 4 ranges
    val numberOfSliceRanges: Int = 4
    val sliceRanges = eventQueries.sliceRanges(numberOfSliceRanges)

    // Example of using the first slice range
    val minSlice: Int = sliceRanges.head.min
    val maxSlice: Int = sliceRanges.head.max
    val entityType: String = "MyEntity"
    eventQueries
      .currentEventsBySlices[MyEvent](entityType, minSlice, maxSlice, NoOffset.getInstance)
      .map(envelope =>
        s"event from persistenceId ${envelope.persistenceId} with " +
        s"seqNr ${envelope.sequenceNr}: ${envelope.event}")
      .runWith(Sink.foreach(println))
    //#currentEventsBySlices
  }

  {
    //#currentChangesBySlices
    import akka.persistence.query.UpdatedDurableState

    // Slit the slices into 4 ranges
    val numberOfSliceRanges: Int = 4
    val sliceRanges = stateQueries.sliceRanges(numberOfSliceRanges)

    // Example of using the first slice range
    val minSlice: Int = sliceRanges.head.min
    val maxSlice: Int = sliceRanges.head.max
    val entityType: String = "MyEntity"
    stateQueries
      .currentChangesBySlices(entityType, minSlice, maxSlice, NoOffset.getInstance)
      .collect { case change: UpdatedDurableState[MyState] => change }
      .map(change =>
        s"state change from persistenceId ${change.persistenceId} with " +
        s"revision ${change.revision}: ${change.value}")
      .runWith(Sink.foreach(println))
    //#currentChangesBySlices
  }
}
