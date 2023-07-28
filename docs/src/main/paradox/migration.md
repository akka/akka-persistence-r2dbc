# Migration tool

There is a migration tool that is useful if you would like to migrate from another Akka Persistence plugin
to the R2DBC plugin. It has been tested with Akka Persistence JDBC as source plugin, but it should work with
any plugin that has support for `CurrentPersistenceIdsQuery` and `CurrentEventsByPersistenceIdQuery`.

The migration tool can be run while the source system is still active, and it can be run multiple times with
idempotent result. Full rolling update when switching database or Persistence plugin is not supported, but
you can migrate most of the data while the system is online and then have a short full shutdown while
migrating the remaining data that was written after the previous online migration.

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
numbers in the table `migration_progress`. In a subsequent run it will only migrate new events and snapshots
compared to what was stored in `migration_progress`. It will also find and migrate new persistence ids in a
subsequent run. You can delete from `migration_progress` if you want to re-run the full migration.

It's recommended that you create the `migration_progress` table before running the migration tool, but
if it doesn't exist the tool will try to create the table.

```sql
CREATE TABLE IF NOT EXISTS migration_progress(
  persistence_id VARCHAR(255) NOT NULL,
  event_seq_nr BIGINT,
  snapshot_seq_nr BIGINT,
  PRIMARY KEY(persistence_id)
```

## Configuration

The migration tool can be run as main class `akka.persistence.r2dbc.migration.MigrationTool` provided by the above
`akka-persistence-r2dbc-migration` dependency.

You need to provide configuration for the source persistence plugin and the target Rd2BC plugin in your `application.conf`. An example of such configuration for migration from Akka Persistence JDBC: 

@@snip [application-postgres.conf](/migration-tests/src/test/resources/application-postgres.conf)

@@@ note

Application specific serializers for events and snapshots must also be configured and included in classpath.

@@@

### Reference configuration

The following can be overridden in your `application.conf` for the migration tool specific settings:

@@snip [reference.conf](/migration/src/main/resources/reference.conf)
