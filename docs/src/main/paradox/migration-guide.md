# Migration Guide

## 1.1.x to 1.2.0

### Configuration file changes
The configuration file structure has changed in an incompatible way (to make room for the H2 dialect), 
an existing project using Postgres or Yugabyte will need the following changes to its config:

Remove `akka.persistence.r2dbc.dialect` from the config if present

Choose dialect by configuring the `connection-factory` block:

Postgres:
```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.postgres}
akka.persistence.r2dbc.connection-factory {
  # only overrides from the default values needs to be defined
  database = "my-postgres-database"
  database = ${?DB_NAME}
}
```

Yugabyte:
```hocon
akka.persistence.r2dbc.connection-factory = ${akka.persistence.r2dbc.yugabyte}
akka.persistence.r2dbc.connection-factory {
  # only overrides from the default values needs to be defined
  database = "my-yugabyte-database"
  database = ${?DB_NAME}
}
```

### API changes
Some accessors on the @apidoc[R2dbcSettings] class has been removed, the `ConnectionFactorySettings` and `Dialect` classes has been removed. A public API for looking at what dialect is used is now provided through `R2dbcSettings.dialectName`.