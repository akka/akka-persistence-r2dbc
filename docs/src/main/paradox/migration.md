# Migration tool

There is a migration tool that is useful if you would like to migrate from another Akka Persistence plugin
to the R2DBC plugin. It has been tested with Akka Persistence JDBC as source plugin, but it should work with
any plugin that has support for `CurrentPersistenceIdsQuery` and `CurrentEventsByPersistenceIdQuery`.

The migration tool can be run while the source system is still active, and it can be run multiple times with
idempotent result. Full rolling update when switching database or Persistence plugin is not supported, but
you can migrate most of the data while the system is online and then have a short full shutdown while
migrating the remaining data that was written after the previous online migration.

The migration tool is intended to run as a separate, standalone application and should not be part of the same jvm process as the main application is running under.

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependency as below.

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-r2dbc-migration_$scala.binary.version$
  version=$project.version$
}

## Progress table

To speed up processing of subsequent runs it stores migrated persistence ids and sequence
numbers in the table `migration_progress`. In a subsequent run it will only migrate new events, snapshots
and durable states compared to what was stored in `migration_progress`. It will also find and migrate
new persistence ids in a subsequent run. You can delete from `migration_progress` if you want to
re-run the full migration.

It's recommended that you create the `migration_progress` table before running the migration tool, but
if it doesn't exist the tool will try to create the table.

Postgres:
: ```sql
CREATE TABLE IF NOT EXISTS migration_progress(
  persistence_id VARCHAR(255) NOT NULL,
  event_seq_nr BIGINT,
  snapshot_seq_nr BIGINT,
  state_revision  BIGINT,
  PRIMARY KEY(persistence_id)
```

SQLServer:
: ```sql
IF object_id('migration_progress') is null
  CREATE TABLE migration_progress(
    persistence_id NVARCHAR(255) NOT NULL,
    event_seq_nr BIGINT,
    snapshot_seq_nr BIGINT,
    state_revision  BIGINT,
    PRIMARY KEY(persistence_id)
```

@@@ warning { .group-sqlserver }

The SQL Server dialect is marked `experimental` and not yet production ready until various [issues](https://github.com/akka/akka-persistence-r2dbc/issues?q=is%3Aopen+label%3Asqlserver+label%3Abug) with the integration of the `r2dbc-mssql` plugin have been resolved.

@@@

## Running

The migration tool can be run as main class `akka.persistence.r2dbc.migration.MigrationTool` provided by the above
`akka-persistence-r2dbc-migration` dependency. The main method will run `MigrationTool.migrateAll`.

@@@ note

Durable State is not migrated by `MigrationTool.migrateAll`, instead you need to use `MigrationTool.migrateDurableStates` for a given list of persistence ids.

@@@

## Configuration

You need to provide configuration for the source persistence plugin and the target Rd2BC plugin in your `application.conf`. An example of such configuration for migration from Akka Persistence JDBC:

Postgres:
: @@snip [application-postgres.conf](/migration-tests/src/test/resources/application-postgres-example.conf)

SQLServer:
: @@snip [application-sqlserver.conf](/migration-tests/src/test/resources/application-sqlserver-example.conf)

@@@ note

Application specific serializers for events and snapshots must also be configured and included in classpath.

@@@

When running the migration tool for Durable State the single writer assertion must be disabled with configuration:
```hcon
akka.persistence.r2dbc.state.assert-single-writer = off
```

### Reference configuration

The following can be overridden in your `application.conf` for the migration tool specific settings:

@@snip [reference.conf](/migration/src/main/resources/reference.conf)
