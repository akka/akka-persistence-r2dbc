# Query Plugin

## Event sourced queries

`R2dbcReadJournal` implements the following @extref:[Persistence Queries](akka:persistence-query.html):

* `eventsByPersistenceId`, `currentEventsByPersistenceId`
* `eventsBySlices`, `currentEventsBySlices`
* `currentPersistenceIds` 

Accessing the `R2dbcReadJournal`:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #readJournalFor } 

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #readJournalFor } 

### eventsByPersistenceId

The `eventsByPersistenceId` and `currentEventsByPersistenceId` queries are useful for retrieving events for a single entity
with a given persistence id.

Example of `currentEventsByPersistenceId`:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #currentEventsByPersistenceId }

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #currentEventsByPersistenceId }

### eventsBySlices

The `eventsBySlices` and `currentEventsBySlices` queries are useful for retrieving all events for a given entity type.
`eventsBySlices` should be used via Akka Projection.

@@@ note

This has historically been done with `eventsByTag` but the R2DBC plugin is instead providing `eventsBySlices`
as an improved solution.

The usage of `eventsByTag` for Projections has the drawback that the number of tags must be decided
up-front and can't easily be changed afterwards. Starting with too many tags means much overhead since
many projection instances would be running on each node in a small Akka Cluster. Each projection instance
polling the database periodically. Starting with too few tags means that it can't be scaled later to more
Akka nodes.

With `eventsBySlices` more Projection instances can be added when needed and still reuse the offsets
for the previous slice distributions.

@@@

A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
persistence ids over the slices. The `eventsBySlices` query is for a range of the slices. For example if
using 128 slices and running 4 Projection instances the slice ranges would be 0-31, 32-63, 64-95, 96-127.
Changing to 8 slice ranges means that the ranges would be 0-15, 16-31, 32-47, ..., 96-111, 112-127.

Example of `currentEventsBySlices`:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #currentEventsBySlices }

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #currentEventsBySlices }

`eventsBySlices` should be used via Akka Projection, which will automatically handle the following
difficulties.

The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
query from a given `offset` after a crash/restart.

The offset is a `TimestampOffset` and it is based on the database `transaction_timestamp()` when the event was stored.
`transaction_timestamp()` is the time when the transaction started, not when it was committed. This means that a
"later" event may be visible first and when retrieving events after the previously seen timestamp may miss some
events. In distributed SQL databases there can also be clock skews for the database timestamps. For that reason
`eventsBySlices` will perform additional backtracking queries to catch missed events. Events from backtracking
will typically be duplicates of previously emitted events. It's the responsibility of the consumer to filter
duplicates and make sure that events are processed in exact sequence number order for each persistence id.
Such deduplication is provided by the R2DBC Projection.

Events emitted by the backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the
consumer can load the full `EventBySliceEnvelope` with [[R2dbcReadJournal.loadEnvelope]].

The events will be emitted in the timestamp order with the caveat of duplicate events as described above. Events
with the same timestamp are ordered by sequence number.

`currentEventsBySlices` doesn't perform these backtracking queries and will not emit duplicates and the
event payload is always full loaded.

## Durable state queries

`R2dbcDurableStateStore` implements the following @extref:[Persistence Queries](akka:durable-state/persistence-query.html):

* `getObject`
* `changesBySlices`, `currentChangesBySlices`
* `currentPersistenceIds`

Accessing the `R2dbcDurableStateStore`:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #durableStateStoreFor }

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #durableStateStoreFor }

### changesBySlices

The `changesBySlices` and `currentChangesBySlices` queries are useful for retrieving updates of the latest durable state
for a given entity type.

Example of `currentChangesBySlices`:

Java
:  @@snip [create](/docs/src/test/java/jdocs/home/query/QueryDocCompileOnly.java) { #currentChangesBySlices }

Scala
:  @@snip [create](/docs/src/test/scala/docs/home/query/QueryDocCompileOnly.scala) { #currentChangesBySlices }

The emitted `DurableStateChange` can be a `UpdatedDurableState` or `DeletedDurableState`, but `DeletedDurableState` is not implemented yet.

It will emit an `UpdatedDurableState` that when the durable state is updated. When the state is updated again another
`UpdatedDurableState` is emitted. It will always emit an `UpdatedDurableState` for the latest revision of the state,
but there is no guarantee that all intermediate changes are emitted if the state is updated several times. Note that
`UpdatedDurableState` contains the full current state, and it is not a delta from previous revision of state.

`changesBySlices` should be used via Akka Projection, which will automatically handle the similar difficulties
with duplicates as described for @ref[eventsBySlices](#eventsbyslices).

## Configuration

Query configuration is under `akka.persistence.r2dbc.query`. Here's the default configuration values for the query plugin:

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #query-settings }

The query plugin shares the connection pool as the rest of the plugin, see @ref:[Connection configuration](connection-config.md).

