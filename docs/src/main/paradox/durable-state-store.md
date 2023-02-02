# Durable state store plugin

The durable state plugin enables storing and loading key-value entries for
@extref:[durable state actors](akka:typed/durable-state/persistence.html).

## Schema

The `durable_state` table and `durable_state_slice_idx` index need to be created in the configured database, see schema
definition in @ref:[Creating the schema](getting-started.md#schema).

The `durable_state_slice_idx` index is only needed if the slice based @ref:[queries](query.md) are used.

## Configuration

To enable the durable state store plugin to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
```

It can also be enabled with the `durableStateStorePluginId` for a specific `DurableStateBehavior` and multiple plugin
configurations are supported.

See also @ref:[Connection configuration](config.md#connection-configuration).

### Reference configuration

The following can be overridden in your `application.conf` for the durable state store specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#durable-state-settings}

## Deletes

The store supports deletes through hard deletes, which means the durable state store entries are actually deleted from
the database. There is no materialized view with a copy of the state so make sure to not delete durable states too early
if they are used from projections or queries.

For each persistent id one tombstone record is kept in the store when the state of the persistence id has been deleted.
The reason for the tombstone record is to keep track of the latest revision number so that subsequent state changes
don't reuse the same revision numbers that have been deleted.

See the @ref[DurableStateCleanup tool](cleanup.md#durable-state-cleanup-tool) for more information about how to delete
state tombstone records.

## Storing query representation

@extref:[Durable state actors](akka:typed/durable-state/persistence.html) can only be looked up by their entity id.
Additional indexed data can be stored as a query representation. You can either store the query representation from
an asynchronous @ref:[Projection](projection.md) or you can store it in the same transaction as the Durable State
upsert or delete.

Advantages of storing the query representation in the same transaction as the Durable State change:

* exactly-once processing and atomic update with the Durable State change
* no eventual consistency delay from asynchronous Projection processing
* no need for Projection offset storage

That said, for write heavy Durable State, a Projection can have the advantage of not impacting write latency of
the Durable State updates. Note that updating the secondary index also has an impact on the write latency.

### Additional columns

In many cases you just want a secondary index on one or a few fields other than the entity id. For that purpose
you can configure one or more @apidoc[AdditionalColumn] classes for an entity type. The `AdditionalColumn` will
extract the field from the Durable State value and define how to bind it to a database column.

The configuration:

@@snip [application.conf](/docs/src/test/scala/docs/home/state/BlogPostTitleColumn.scala) { #additional-column-config }

For each entity type you can define a list of fully qualified class names of `AdditionalColumn` implementations.
The `AdditionalColumn` implementation may optionally define an ActorSystem constructor parameter.

`AdditionalColumn` for a secondary index on the title of @extref:[blog posts (example in the Akka documentation)](akka:typed/durable-state/persistence.html#changing-behavior):

Scala
:  @@snip [BlogPostTitleColumn.scala](/docs/src/test/scala/docs/home/state/BlogPostTitleColumn.scala) { #additional-column }

Java
:  @@snip [BlogPostTitleColumn.java](/docs/src/test/java/jdocs/home/state/BlogPostTitleColumn.java) { #additional-column }

From the `bind` method you can return one of:

* @scala[`AdditionalColumn.BindValue`]@java[`AdditionalColumn.bindValue`] - bind a value such as a `String` or `Long` to the database column
* @scala[`AdditionalColumn.BindNull`]@java[`AdditionalColumn.bindNull`] - store `null` in the database column
* @scala[`AdditionalColumn.Skip`]@java[`AdditionalColumn.skip`] - don't update the database column for this change, keep existing value

You would have to add the additional columns to the `durable_state` table definition, and create secondary database index.
Unless you only have one entity type it's best to define a separate table with the `custom-table` configuration, see
example above. The full serialized state and the additional columns are stored in the custom table instead of the
default `durable_state` table. The custom table should have the same table definition as the default 
`durable_state` table but with the extra columns added.

The state can be found by the additional column and deserialized like this:

Scala
:  @@snip [BlogPostQuery.scala](/docs/src/test/scala/docs/home/state/BlogPostQuery.scala) { #query }

Java
:  @@snip [BlogPostQuery.java](/docs/src/test/java/jdocs/home/state/BlogPostQuery.java) { #query }

### Change handler

For more advanced cases where the query representation would not fit in @ref:[additional columns](#additional-columns)
you can configure a @apidoc[ChangeHandler] for an entity type. The `ChangeHandler` will be invoked for each
Durable State change. From the `ChangeHandler` you can run database operations in the same transaction
as the Durable State upsert or delete.

The configuration:

@@snip [application.conf](/docs/src/test/scala/docs/home/state/BlogPostCounts.scala) { #change-handler-config }

For each entity type you can define the fully qualified class name of a `ChangeHandler` implementation.
The `ChangeHandler` implementation may optionally define an ActorSystem constructor parameter.

`ChangeHandler` for a keeping track of number of published @extref:[blog posts (example in the Akka documentation)](akka:typed/durable-state/persistence.html#changing-behavior):

Scala
:  @@snip [BlogPostCounts.scala](/docs/src/test/scala/docs/home/state/BlogPostCounts.scala) { #change-handler }

Java
:  @@snip [BlogPostCounts.java](/docs/src/test/java/jdocs/home/state/BlogPostCounts.java) { #change-handler }

The @apidoc[DurableStateChange] parameter is an @apidoc[UpdatedDurableState] when the Durable State is created or updated.
It is a @apidoc[DeletedDurableState] when the Durable State is deleted.

The @apidoc[akka.persistence.r2dbc.session.*.R2dbcSession] provides the means to access an open R2DBC connection
that can be used to process the change. The target database operations run in the same transaction as the storage
of the Durable State change.

One change is processed at a time. It will not be invoked with the next change until after the `process` method returns
and the returned @scala[`Future`]@java[`CompletionStage`] is completed.

@@@ note { title="Concurrency semantics" }

The `ChangeHandler` should be implemented as a stateless function without mutable state because the same
`ChangeHandler` instance may be invoked concurrently for different entities.
For a specific entity (persistenceId) one change is processed at a time and the `process` method will not be
invoked with the next change for that entity until after the returned @scala[`Future`]@java[`CompletionStage`] is completed.

@@@
