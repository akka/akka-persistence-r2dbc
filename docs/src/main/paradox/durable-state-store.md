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

See also @ref:[Connection configuration](connection-config.md).

### Reference configuration

The following can be overridden in your `application.conf` for the durable state store specific settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#durable-state-settings}

## Deletes

The store supports deletes through hard deletes, which means the durable state store entries are actually deleted from
the database. There is no materialized view with a copy of the state so make sure to not delete durable states too early
if they are used from projections or queries.

For each persistent id one tombstone record is kept in the store when the state of the persistence id has been
deleted. The reason for the tombstone record is to keep track of the latest revision number so that subsequent state
changes don't reuse same revision numbers that have been deleted.

See the @ref[DurableStateCleanup tool](cleanup.md#durable-state-cleanup-tool) for more information about how to delete
state tombstone records.
