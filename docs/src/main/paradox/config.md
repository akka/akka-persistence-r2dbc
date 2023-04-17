# Configuration

## Connection configuration

Shared configuration for the connection pool is located under `akka.persistence.r2dbc.connection-factory`. You have to set at least:

Postgres:
: @@snip [application.conf](/docs/src/test/resources/application-postgres.conf) { #connection-settings }

Yugabyte:
: @@snip [application.conf](/docs/src/test/resources/application-yugabyte.conf) { #connection-settings }

The following can be overridden in your `application.conf` for the connection settings:

@@snip [reference.conf](/core/src/main/resources/reference.conf) {#connection-settings}

## Journal configuration

Journal configuration properties are by default defined under `akka.persistence.r2dbc.journal`.

See @ref:[Journal plugin configuration](journal.md#configuration).

## Snapshot configuration

Snapshot store configuration properties are by default defined under `akka.persistence.r2dbc.snapshot`.

See @ref:[Snapshot store plugin configuration](snapshots.md#configuration).

## Durable state configuration

Durable state store configuration properties are by default defined under `akka.persistence.r2dbc.state`.

See @ref:[Durable state plugin configuration](durable-state-store.md#configuration).

## Query configuration

Query configuration properties are by default defined under `akka.persistence.r2dbc.query`.

See @ref:[Query plugin configuration](query.md#configuration).

## Multiple plugins

To enable the plugins to be used by default, add the following lines to your Akka `application.conf`:

@@snip [application.conf](/core/src/test/scala/akka/persistence/r2dbc/journal/MultiPluginSpec.scala) {#default-config}

Note that all plugins have a shared root config section `akka.persistence.r2dbc`, which also contains the
@ref:[Connection configuration](#connection-configuration) for the connection pool that is shared for the plugins.

You can use additional plugins with different configuration. For example if more than one database is used. Then you would define the configuration
such as:

@@snip [application.conf](/core/src/test/scala/akka/persistence/r2dbc/journal/MultiPluginSpec.scala) {#second-config}

To use the additional plugin you would @scala[define]@java[override] the plugin id.

Scala
:  @@snip [MultiPluginDocExample.scala](/core/src/test/scala/akka/persistence/r2dbc/journal/MultiPluginSpec.scala){#withPlugins}

Java
:  @@snip [MultiPluginDocExample.java](/docs/src/test/java/jdocs/home/MultiPluginDocExample.java) {#withPlugins}

It is similar for `DurableStateBehavior`, @scala[define `withDurableStateStorePluginId("second-r2dbc.state")`]
@java[override `durableStateStorePluginId` with `"second-r2dbc.state"`].

For queries and Projection `SourceProvider` you would use `"second-r2dbc.query"` instead of the default @scala[`R2dbcReadJournal.Identifier`]
@java[`R2dbcReadJournal.Identifier()`] (`"akka.persistence.r2dbc.query"`).

For additional details on multiple plugin configuration for projections see @extref:[the Akka R2DBC projection docs](akka-projection:r2dbc.html#multiple-plugins)
