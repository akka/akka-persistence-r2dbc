# Journal plugin

The journal plugin enables storing and loading events for
@extref:[event sourced persistent actors](akka:typed/persistence.html).

## Schema

The `event_journal` table and `event_journal_slice_idx` index need to be created in the configured database, see schema
definition in @ref:[Creating the schema](getting-started.md#schema).

The `event_journal_slice_idx` index is only needed if the slice based @ref:[queries](query.md) are used.

## Configuration

To enable the journal plugin to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
```

It can also be enabled with the `journalPluginId` for a specific `EventSourcedBehavior` and multiple plugin
configurations are supported.

See also @ref:[Connection configuration](config.md#connection-configuration).

### Reference configuration

The following can be overridden in your `application.conf` for the journal specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#journal-settings}

## Event serialization

The events are serialized with @extref:[Akka Serialization](akka:serialization.html) and the binary representation
is stored in the `event_payload` column together with information about what serializer that was used in the
`event_ser_id` and `event_ser_manifest` columns.

For PostgreSQL the payload is stored as `BYTEA` type. Alternatively, you can use `JSONB` column type as described in
@ref:[PostgreSQL JSON](postgres_json.md).

## Deletes

The journal supports deletes through hard deletes, which means the journal entries are actually deleted from the
database. There is no materialized view with a copy of the event so make sure to not delete events too early if they are
used from projections or queries.

For each persistent id one tombstone record is kept in the event journal when all events of a persistence id have been
deleted. The reason for the tombstone record is to keep track of the latest sequence number so that subsequent events
don't reuse the same sequence numbers that have been deleted.

See the @ref[EventSourcedCleanup tool](cleanup.md#event-sourced-cleanup-tool) for more information about how to delete
events, snapshots and tombstone records.

## Storing query representation

Events can be looked up by their persistence id. Additional indexed data can be stored alongside each event so
the event journal can be queried by a secondary index. The most common use case is a `JSONB` column that holds
application-level metadata you want to query with a GIN or expression index.

The journal supports this through @apidoc[akka.persistence.r2dbc.journal.*.AdditionalColumn] (configured per
entity type). For more elaborate query representations you can use a @ref:[Projection](projection.md) to derive a
separate query representation asynchronously.

### Additional columns

For each event being persisted, an `AdditionalColumn` extracts a field from the event (and its metadata/tags) and binds
the value to an extra column on the event journal table. The column can be queried independently of the primary key.

The configuration:

@@snip [application.conf](/docs/src/test/scala/docs/home/journal/EventTitleColumn.scala) { #additional-column-config }

For each entity type you can define a list of fully qualified class names of `AdditionalColumn` implementations. The
`AdditionalColumn` implementation may optionally define an `ActorSystem` constructor parameter.

An `AdditionalColumn` that stores the `title` of an event:

Scala
:  @@snip [EventTitleColumn.scala](/docs/src/test/scala/docs/home/journal/EventTitleColumn.scala) { #additional-column }

Java
:  @@snip [EventTitleColumn.java](/docs/src/test/java/jdocs/home/journal/EventTitleColumn.java) { #additional-column }

From the `bind` method you can return one of:

* @scala[`AdditionalColumn.BindValue`]@java[`AdditionalColumn.bindValue`] — bind a value such as a `String` or `Long` to the database column
* @scala[`AdditionalColumn.BindNull`]@java[`AdditionalColumn.bindNull`] — store `NULL` in the database column
* @scala[`AdditionalColumn.Skip`]@java[`AdditionalColumn.skip`] — omit the column from the INSERT; the column's `DEFAULT` (usually `NULL`) is used

The `Insert` payload passed to `bind` exposes `persistenceId`, `entityType`, `slice`, `seqNr`, the deserialized event
`value`, the deserialized event `metadata`, and the event `tags`. Use these to build whatever column value the secondary
query needs.

You would have to add the additional columns to the `event_journal` table definition and create the secondary database
index. Unless you only have one entity type it is best to define a separate journal table with the `custom-table`
configuration (see @ref:[Custom journal table per entity type](#custom-journal-table-per-entity-type)) so the extra
column does not need to be added to the shared `event_journal` table.

A few things to call out:

* The additional column must be `NULL`-able. Delete-marker (tombstone) rows do **not** invoke `bind`, so they leave the
  column at its `DEFAULT`.
* Within an atomic persist of several events, all must produce the same shape (same `Skip`/`BindNull`/`BindValue` decisions
  per column). A mismatch fails the write fast with an `IllegalArgumentException`.

The events can be found by the additional column and deserialized like this:

Scala
:  @@snip [BlogPostQuery.scala](/docs/src/test/scala/docs/home/journal/BlogPostQuery.scala) { #query }

Java
:  @@snip [BlogPostQuery.java](/docs/src/test/java/jdocs/home/journal/BlogPostQuery.java) { #query }

#### Additional column as PostgreSQL JSON

With PostgreSQL the additional column type can be `JSONB` to take advantage of PostgreSQL support for
[JSON Types](https://www.postgresql.org/docs/current/datatype-json.html). Wrap the string or byte array representation
of the JSON in `io.r2dbc.postgresql.codec.Json` when binding the value:

Scala
:  @@snip [EventJsonColumn.scala](/docs/src/test/scala/docs/home/journal/EventJsonColumn.scala) { #additional-column-json }

Java
:  @@snip [EventJsonColumn.java](/docs/src/test/java/jdocs/home/journal/EventJsonColumn.java) { #additional-column-json }

Add a JSONB column and a GIN index to the journal table, then query with the usual `column @> '{...}'::jsonb` or
`column->>'field' = ?` operators.

See also @ref:[PostgreSQL JSON](postgres_json.md).

#### Additional column schema with H2

As H2 runs in-process it is not possible to create the schema up front, so additional columns to the journal table can
be created through the `additional-init` setting:

@@snip [Sample config](/core/src/test/scala/akka/persistence/r2dbc/internal/H2JournalAdditionalInitForSchemaSpec.scala) { #additionalColumnJournal }

### Custom journal table per entity type

For each entity type you can route writes and reads to a separate journal table. This is the recommended approach when
the additional columns for an entity type should not be added to the shared `event_journal` table.

```hocon
akka.persistence.r2dbc.journal {
  custom-table {
    "BlogPost" = event_journal_blogpost
  }
}
```

The custom table must have the same column layout as the default `event_journal` table, plus any additional columns the
entity type needs. Slice indexes, schema prefix, and data partition suffixes are applied the same way as on the default
table.
