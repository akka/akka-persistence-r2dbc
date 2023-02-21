/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala213 = "2.13.10"
  val Scala212 = "2.12.17"
  val Scala3 = "3.1.3"
  val Scala2Versions = Seq(Scala213, Scala212)
  val ScalaVersions = Dependencies.Scala2Versions :+ Dependencies.Scala3
  val AkkaVersion = System.getProperty("override.akka.version", "2.8.0-M4")
  val AkkaVersionInDocs = AkkaVersion.take(3)
  val AkkaProjectionVersion = "1.4.0-M2-31-0490fe4e-SNAPSHOT"
  val AkkaPersistenceJdbcVersion = "5.2.0" // only in migration tool tests
  val AkkaProjectionVersionInDocs = "current"

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion

    val akkaProjectionCore = "com.lightbend.akka" %% "akka-projection-core" % AkkaProjectionVersion
    val akkaProjectionGrpc = "com.lightbend.akka" %% "akka-projection-grpc" % AkkaProjectionVersion % Provided

    val r2dbcSpi = "io.r2dbc" % "r2dbc-spi" % "1.0.0.RELEASE" // ApacheV2
    val r2dbcPool = "io.r2dbc" % "r2dbc-pool" % "1.0.0.RELEASE" // ApacheV2
    val r2dbcPostgres = "org.postgresql" % "r2dbc-postgresql" % "1.0.1.RELEASE" // ApacheV2
  }

  object TestDeps {
    val akkaStreamTyped = "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion % Test
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion % Test
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion % Test
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test
    // Note: Not sure why this is needed, but the projection tests fail with mixed akka module versions without it
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % AkkaVersion % Test

    val akkaProjectionEventSourced =
      "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion % Test
    val akkaProjectionDurableState =
      "com.lightbend.akka" %% "akka-projection-durable-state" % AkkaProjectionVersion % Test
    val akkaProjectionTestKit = "com.lightbend.akka" %% "akka-projection-testkit" % AkkaProjectionVersion % Test

    val postgresql = "org.postgresql" % "postgresql" % "42.5.2" % Test // BSD-2-Clause

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.11" % Test // EPL 1.0 / LGPL 2.1
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.12" % Test // ApacheV2
    val junit = "junit" % "junit" % "4.12" % Test // Eclipse Public License 1.0
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % Test // "BSD 2-Clause"
  }

  import Compile._

  val core = Seq(
    akkaPersistence,
    akkaPersistenceQuery,
    r2dbcSpi,
    r2dbcPool,
    r2dbcPostgres,
    TestDeps.akkaPersistenceTck,
    TestDeps.akkaStreamTestkit,
    TestDeps.akkaTestkit,
    TestDeps.akkaJackson,
    TestDeps.akkaStreamTyped,
    TestDeps.logback,
    TestDeps.scalaTest)

  val projection = Seq(
    akkaPersistenceQuery,
    r2dbcSpi,
    r2dbcPool,
    r2dbcPostgres,
    akkaProjectionCore,
    akkaProjectionGrpc,
    TestDeps.akkaProjectionEventSourced,
    TestDeps.akkaProjectionDurableState,
    TestDeps.akkaStreamTestkit,
    TestDeps.akkaTestkit,
    TestDeps.akkaProjectionTestKit,
    TestDeps.akkaJackson,
    TestDeps.akkaDiscovery,
    TestDeps.logback,
    TestDeps.scalaTest)

  val migrationTests =
    Seq(
      "com.lightbend.akka" %% "akka-persistence-jdbc" % AkkaPersistenceJdbcVersion % Test,
      TestDeps.postgresql,
      TestDeps.logback,
      TestDeps.scalaTest)

  val docs =
    Seq(
      TestDeps.akkaPersistenceTyped,
      TestDeps.akkaProjectionEventSourced,
      TestDeps.akkaProjectionDurableState,
      TestDeps.akkaShardingTyped)
}
