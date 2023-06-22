# Migration Guide

## 1.1.x to 1.2.0

### Configuration file changes
The configuration file structure has changed in an incompatible way (to make room for the H2 dialect), 
an existing project using Postgres or Yugabyte will need the following changes to its config:

Remove `akka.persistence.r2dbc.dialect` from the config if present

Choose dialect by configuring the `connection-factory` block:

Postgres:
: ```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.postgres}
akka.persistence.r2dbc.connection-factory {
  # only overrides from the default values needs to be defined
  database = "my-postgres-database"
  database = ${?DB_NAME}
}
```

Yugabyte:
: ```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.yugabyte}
akka.persistence.r2dbc.connection-factory {
  # only overrides from the default values needs to be defined
  database = "my-yugabyte-database"
  database = ${?DB_NAME}
}
```

### API changes
Some accessors on the @apidoc[R2dbcSettings] class has been removed, the `ConnectionFactorySettings` and `Dialect` classes has been removed. A public API for looking at what dialect is used is now provided through `R2dbcSettings.dialectName`.

<a id="eventsBySlicesStartingFromSnapshots"></a>

### Optional changes for eventsBySlicesStartingFromSnapshots

These changes are only needed if you use the @ref:[new feature of using snapshots as starting points](query.md#eventsbyslicesstartingfromsnapshots)
for `eventsBySlices` queries.

The `snapshot` table must be altered to add two new columns:

Postgres:
: ```sql
ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS db_timestamp timestamp with time zone;
ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS tags TEXT ARRAY;
```

Yugabyte:
: ```sql
ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS db_timestamp timestamp with time zone;
ALTER TABLE snapshot ADD COLUMN IF NOT EXISTS tags TEXT ARRAY;
```

Populate the two new columns in the `snapshot` table from corresponding events in the `event_journal` table.

Postgres:
: ```sql
UPDATE snapshot s SET db_timestamp = e.db_timestamp, tags = e.tags FROM event_journal e WHERE s.persistence_id = e.persistence_id and s.seq_nr = e.seq_nr;
```

Yugabyte:
: ```sql
UPDATE snapshot s SET db_timestamp = e.db_timestamp, tags = e.tags FROM event_journal e WHERE s.persistence_id = e.persistence_id and s.seq_nr = e.seq_nr;
```

A new index must be added to the `snapshot` table:

Postgres:
: ```sql
CREATE INDEX IF NOT EXISTS snapshot_slice_idx ON snapshot(slice, entity_type, db_timestamp);
```

Yugabyte:
: ```sql
CREATE INDEX IF NOT EXISTS snapshot_slice_idx ON snapshot(slice ASC, entity_type ASC, db_timestamp ASC, persistence_id)
  SPLIT AT VALUES ((127), (255), (383), (511), (639), (767), (895));
```

The feature must be enabled with configuration:

```hocon
akka.persistence.r2dbc.query.start-from-snapshot.enabled = true
```

These changes can be made in a rolling update process.

1. While running the old version, alter tables to add the two columns.
2. Enable configuration and roll out new version. Don't use `eventsBySlicesStartingFromSnapshots` yet.
3. Populate the columns with the update statement. Create the index.
4. Roll out another version where you can use `eventsBySlicesStartingFromSnapshots`.
