# Overview

The Akka Persistence R2DBC plugin allows for using SQL database with R2DBC as a backend for Akka Persistence.

Currently, the R2DBC plugin has support for:

 * [PostgreSQL](https://www.postgresql.org) 
 * [Yugabyte](https://www.yugabyte.com)
 * [H2](https://h2database.com) - As a minimal in-process memory or file based database.
 * [Microsoft SQL Server](https://microsoft.com/sqlserver)

It is specifically designed to work well for distributed SQL databases.

[Create an issue](https://github.com/akka/akka-persistence-r2dbc/issues) if you would like to @ref[contribute](contributing.md)
support for other databases that has a [R2DBC driver](https://r2dbc.io/drivers/).

## Project Info

@@project-info{ projectId="core" }

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

@@dependencies{ projectId="core" }
