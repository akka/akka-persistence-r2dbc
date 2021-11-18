# Overview

The Akka Persistence R2DBC plugin allows for using SQL database with R2DBC as a backend for Akka Persistence.

Currently, the R2DBC plugin has support for [PostgreSQL](https://www.postgresql.org) and [Yugabyte](https://www.yugabyte.com).
It is specifically designed to work well for distributed SQL databases.

[Create an issue](https://github.com/akka/akka-persistence-r2dbc/issues) if you would like to @ref[contribute](contributing.md)
support for other databases that has a [R2DBC driver](https://r2dbc.io/drivers/).

@@@ warning

The project is currently under development and there are no guarantees for binary compatibility
and the schema may change.

@@@

## Project Info

@@project-info{ projectId="core" }

## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.lightbend.akka
  artifact=akka-persistence-r2dbc_$scala.binary.version$
  version=$project.version$
}

This plugin depends on Akka $akka.version$ or later, and note that it is important that all `akka-*` 
dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems 
with transient dependencies causing an unlucky mix of versions.

@@dependencies{ projectId="core" }


