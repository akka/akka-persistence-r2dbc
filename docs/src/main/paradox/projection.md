# Projection

The @apidoc[R2dbcProjection$] has support for storing the offset in a relational database using R2DBC.

The source of the envelopes is from a `SourceProvider`, which can be:

* events from Event Sourced entities via the @extref:[SourceProvider for eventsBySlices](akka-projection:eventsourced.html#sourceprovider-for-eventsbyslices) with the @ref:[eventsBySlices query](query.md#eventsbyslices)
* state changes for Durable State entities via the @extref:[SourceProvider for changesBySlices](akka-projection:durable-state.html#sourceprovider-for-changesbyslices) with the @ref:[changesBySlices query](query.md#changesbyslices)
* any other `SourceProvider` with supported @ref:[offset types](#offset-types)

A @apidoc[R2dbcHandler] receives a @apidoc[R2dbcSession] instance and an envelope. The 
`R2dbcSession` provides the means to access an open R2DBC connection that can be used to process the envelope.
The target database operations can be run in the same transaction as the storage of the offset, which means 
that @ref:[exactly-once](#exactly-once) processing semantics is supported. It also offers
@ref:[at-least-once](#at-least-once) semantics.

## Dependencies

To use the R2DBC module of Akka Projections add the following dependency in your project:

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-r2dbc_$scala.binary.version$
  version=$project.version$
}

Akka Projections R2DBC depends on Akka $akka.version$ or later, and note that it is important that all `akka-*`
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems
with transient dependencies causing an unlucky mix of versions.

@@project-info{ projectId="projection" }


### Transitive dependencies

The table below shows `akka-projection-r2dbc`'s direct dependencies, and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="projection" }

## Schema

The `akka_projection_offset_store`, `akka_projection_timestamp_offset_store` and `akka_projection_management` tables
need to be created in the configured database, see schema definition in @ref:[Creating the schema](getting-started.md#schema).

## Configuration

By default, `akka-projection-r2dbc` uses the same connection pool and `dialect` as `akka-persistence-r2dbc`, see
@ref:[Connection configuration](connection-config.md).

### Reference configuration

The following can be overridden in your `application.conf` for the Projection specific settings:

@@snip [reference.conf](/projection/src/main/resources/reference.conf) {#projection-config}

## Running with Sharded Daemon Process

The Sharded Daemon Process can be used to distribute `n` instances of a given Projection across the cluster.
Therefore, it's important that each Projection instance consumes a subset of the stream of envelopes.

When using `eventsBySlices` the initialization code looks like this:

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #initProjections }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #initProjections }

The @ref:[`ShoppingCartHandler` is shown below](#handler).

There are alternative ways of running the `ProjectionBehavior` as described in @extref:[Running a Projection](akka-projection:running.html), but note that when using the R2DBC plugin as `SourceProvider` it is recommended to use `eventsBySlices` and not `eventsByTag`.

## Slices

The `SourceProvider` for Event Sourced actors has historically been using `eventsByTag` but the R2DBC plugin is
instead providing `eventsBySlices` as an improved solution.

The usage of `eventsByTag` for Projections has the drawback that the number of tags must be decided
up-front and can't easily be changed afterwards. Starting with too many tags means much overhead since
many projection instances would be running on each node in a small Akka Cluster. Each projection instance
polling the database periodically. Starting with too few tags means that it can't be scaled later to more
Akka nodes.

With `eventsBySlices` more Projection instances can be added when needed and still reuse the offsets
for the previous slice distributions.

A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
persistence ids over the slices. The `eventsBySlices` query is for a range of the slices. For example if
using 1024 slices and running 4 Projection instances the slice ranges would be 0-255, 256-511, 512-767, 768-1023.
Changing to 8 slice ranges means that the ranges would be 0-127, 128-255, 256-383, ..., 768-895, 896-1023.

However, when changing the number of slices the projections with the old slice distribution must be
stopped before starting new projections. That can be done with a full shutdown before deploying the
new slice distribution or pause (stop) the projections with @extref:[the management API](akka-projection:management.html).

When using `R2dbcProjection` together with the `EventSourcedProvider.eventsBySlices` the events will be delivered in
sequence number order without duplicates.

When using `R2dbcProjection` together with `DurableStateSourceProvider.changesBySlices` the changes will be delivered
in revision number order without duplicates.

## exactly-once

The offset is stored in the same transaction used for the user defined `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #exactlyOnce }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #exactlyOnce }

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## at-least-once

The offset is stored after the envelope has been processed and giving at-least-once processing semantics.
This means that if the projection is restarted from a previously stored offset some elements may be processed more
than once. Therefore, the @ref:[Handler](#handler) code must be idempotent.

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #atLeastOnce }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #atLeastOnce }

The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withSaveOffset` of the returned `AtLeastOnceProjection`.
The default settings for the window is defined in configuration section `akka.projection.at-least-once`.
There is a performance benefit of not storing the offset too often, but the drawback is that there can be more
duplicates when the projection that will be processed again when the projection is restarted.

The @ref:[`ShoppingCartHandler` is shown below](#handler).

## groupedWithin

The envelopes can be grouped before processing, which can be useful for batch updates.

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #grouped }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #grouped }

The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first.
This window can be defined with `withGroup` of the returned `GroupedProjection`. The default settings for
the window is defined in configuration section `akka.projection.grouped`.

When using `groupedWithin` the handler is a @scala[`R2dbcHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]]`]@java[`R2dbcHandler<List<EventEnvelope<ShoppingCart.Event>>>`].
The @ref:[`GroupedShoppingCartHandler` is shown below](#grouped-handler).

The offset is stored in the same transaction used for the user defined `handler`, which means exactly-once
processing semantics if the projection is restarted from previously stored offset.

## Handler

It's in the @apidoc[R2dbcHandler] that you implement the processing of each envelope. It's essentially a consumer function
from `(R2dbcSession, Envelope)` to @scala[`Future[Done]`]@java[`CompletionStage<Done>`]. 

A handler that is consuming `ShoppingCart.Event` from `eventsBySlices` can look like this:

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #handler }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #handler }

@@@ note { title=Hint }
Such simple handlers can also be defined as plain functions via the helper @scala[`R2dbcHandler.apply`]@java[`R2dbcHandler.fromFunction`] factory method.
@@@

### Grouped handler

When using @ref:[`R2dbcProjection.groupedWithin`](#groupedwithin) the handler is processing a @scala[`Seq`]@java[`List`] of envelopes.

Scala
:  @@snip [R2dbcProjectionDocExample.scala](/docs/src/test/scala/docs/home/projection/R2dbcProjectionDocExample.scala) { #grouped-handler }

Java
:  @@snip [R2dbcProjectionDocExample.java](/docs/src/test/java/jdocs/home/projection/R2dbcProjectionDocExample.java) { #grouped-handler }

### Stateful handler

The @apidoc[R2dbcHandler] can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery
one envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile
or other concurrency primitives are needed for managing the state as long as it's not accessed by other threads
than the one that called `process`.

@@@ note

It is important that the `Handler` instance is not shared between several `Projection` instances,
because then it would be invoked concurrently, which is not how it is intended to be used. Each `Projection`
instance should use a new `Handler` instance.  

@@@

### Async handler

The @apidoc[Handler] can be used with `R2dbcProjection.atLeastOnceAsync` and 
`R2dbcProjection.groupedWithinAsync` if the handler is not storing the projection result in the database.
The handler could send to a Kafka topic or integrate with something else.

There are several examples of such `Handler` in the @extref:[documentation for Cassandra Projections](akka-projection:cassandra.html#handler).
Same type of handlers can be used with `R2dbcProjection` instead of `CassandraProjection`.

### Actor handler

A good alternative for advanced state management is to implement the handler as an 
@extref:[actor](akka:typed/typed/actors.html) which is described in 
@extref:[Processing with Actor](akka-projection:actor.html).

### Flow handler

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes,
which is described in @extref:[Processing with Akka Streams](akka-projection:flow.html).

### Handler lifecycle

You can override the `start` and `stop` methods of the `R2dbcHandler` to implement initialization
before first envelope is processed and resource cleanup when the projection is stopped.
Those methods are also called when the `Projection` is restarted after failure.

See also @extref:[error handling](akka-projection:error.html).

## Offset types

The supported offset types of the `R2dbcProjection` are:

* @apidoc[akka.persistence.query.TimestampOffset] that is used for @extref:[SourceProvider for eventsBySlices](akka-projection:eventsourced.html#sourceprovider-for-eventsbyslices) and @extref:[SourceProvider for changesBySlices](akka-projection:durable-state.html#sourceprovider-for-changesbyslices)
* other @apidoc[akka.persistence.query.Offset] types
* @apidoc[MergeableOffset] that is used for @extref:[messages from Kafka](akka-projection:kafka.md#mergeable-offset)
* `String`
* `Int`
* `Long`
* Any other type that has a configured Akka Serializer is stored with base64 encoding of the serialized bytes.

## Publish events for lower latency

To reduce the latency until the Projection finds and process new events you can enable the feature described in @ref:[eventsBySlices documentation](query.md#publish-events-for-lower-latency-of-eventsbyslices).
