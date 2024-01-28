# Getting Started

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
  artifact=akka-persistence-r2dbc_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka $akka.version$ or later, and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

The plugin is published for Scala 2.13 and 2.12.

## Enabling

To enable the plugins to be used by default, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
```

More information about each individual plugin in:

* @ref:[journal](journal.md)
* @ref:[snapshot store](snapshots.md)
* @ref:[durable state store](durable-state-store.md)
* @ref:[queries](query.md)

### Selecting database dialect

You will also need to configure which database dialect to use:

#### Postgres

A transitive dependency pulls in Postgres drivers by default. To use Postgres you only configure the connection factory to the default Postgres block:

```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.postgres}
akka.persistence.r2dbc.connection-factory = {
  # overrides for default values from the 'akka.persistence.r2dbc.postgres' config block
  user = "myuser"
}
```

See @ref[Configuration](config.md) for more configuration details.

#### Yugabyte

A transitive dependency pulls in Postgres drivers that can be used to connect to Yugabyte by default. To use Yugabyte you only configure the connection factory to the default Yugabyte block:

```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.yugabyte}
akka.persistence.r2dbc.connection-factory = {
  # overrides for default values from the 'akka.persistence.r2dbc.yugabyte' config block
  user = "myuser"
}
```

See @ref[Configuration](config.md) for more configuration details.

#### Using H2

The H2 dependencies are marked as `provided` dependencies of `akka-persistence-r2dbc` to not be pulled in for projects not using H2. They must be listed explicitly as dependencies in the build configuration for projects that use them. The two required artifacts are:

@@dependency [Maven,sbt,Gradle] {
  group=com.h2database
  artifact=h2
  version=$h2.version$
  group2=io.r2dbc
  artifact2=r2dbc-h2
  version2=$r2dbc-h2.version$
}

With the dependencies added to your project, configure the connection factory to the default H2 block:

```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.h2}
akka.persistence.r2dbc.connection-factory = {
  # overrides for default values from the 'akka.persistence.r2dbc.h2' config block
  protocol = "mem"
  database = "mydb"
}
```

See @ref[Configuration](config.md) for more configuration details.

#### Using Microsoft SQL Server

The SQL Server dependency is marked as `provided` dependencies of `akka-persistence-r2dbc` to not be pulled in for projects not using SQL Server. It must be listed explicitly as dependencies in the build configuration for projects that use it. The required artifacts is:

@@dependency [Maven,sbt,Gradle] {
group=io.r2dbc
artifact=r2dbc-mssql_$scala.binary.version$
version=$sqlserver.version$
}

With the dependencies added to your project, configure the connection factory to the default SQL Server block:

```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.sqlserver}
akka.persistence.r2dbc.connection-factory = {
  # overrides for default values from the 'akka.persistence.r2dbc.sqlserver' config block
  user = "myuser"
}
```

See @ref[Configuration](config.md) for more configuration details.

## Local testing with docker

The database can be run in Docker. Here's a sample docker compose file:

Postgres:
: @@snip [docker-compose.yml](/docker/docker-compose-postgres.yml)

Yugabyte:
: @@snip [docker-compose.yml](/docker/docker-compose-yugabyte.yml)

Start with:

Postgres:
: ```
docker-compose -f docker/docker-compose-postgres.yml up --wait
```

Yugabyte:
: ```
docker-compose -f docker/docker-compose-yugabyte.yml up
```

<a id="schema"></a>
### Creating the schema

Tables and indexes:

Postgres:
: @@snip [create_tables.sql](/ddl-scripts/create_tables_postgres.sql)

Postgres JSONB:
: @@snip [create_tables.sql](/ddl-scripts/create_tables_postgres_jsonb.sql)

Yugabyte:
: @@snip [create_tables.sql](/ddl-scripts/create_tables_yugabyte.sql)

The ddl script can be run in Docker with:

Postgres:
: ```
docker exec -i postgres-db psql -U postgres -t < ddl-scripts/create_tables_postgres.sql
```

Yugabyte:
: ```
docker exec -i yb-tserver-n1 /home/yugabyte/bin/ysqlsh -h yb-tserver-n1 -t < ddl-scripts/create_tables_yugabyte.sql
```

### Dropping the schema

Postgres:
: @@snip [drop_tables.sql](/ddl-scripts/drop_tables_postgres.sql)

Yugabyte:
: @@snip [drop_tables.sql](/ddl-scripts/drop_tables_postgres.sql)

### Local testing in process with H2

H2 runs in the JVM process, either using a database directly in memory or in a local file. This means it requires no additional steps to start a database, it is started on first connection from the Akka Persistence R2DBC plugin.

The database for H2 is created on first connection. Additional database schema creation or changes can be applied using the setting `additional-init` setting.

Note that it is not possible to share the file based database storage between processes, usage in an Akka cluster is not possible. For other usages where several processes may run at the same time (for example a CI server) it is important to make sure each new process will use a separate file not shared with other processes.