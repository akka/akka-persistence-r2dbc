/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala212 = "2.12.16"
  val Scala213 = "2.13.10"
  val AkkaVersion = System.getProperty("override.akka.version", "2.7.0-M3")
  val AkkaVersionInDocs = AkkaVersion.take(3)
  val AkkaProjectionVersion = "1.3.0-M3"
  val AkkaProjectionVersionInDocs = "current"

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion

    val akkaProjectionCore = "com.lightbend.akka" %% "akka-projection-core" % AkkaProjectionVersion

    val r2dbcSpi = "io.r2dbc" % "r2dbc-spi" % "0.9.1.RELEASE"
    val r2dbcPool = "io.r2dbc" % "r2dbc-pool" % "0.9.2.RELEASE"
    val r2dbcPostgres = "org.postgresql" % "r2dbc-postgresql" % "0.9.2.RELEASE"
  }

  object TestDeps {
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion % Test
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion % Test
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test

    val akkaProjectionEventSourced =
      "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion % Test
    val akkaProjectionDurableState =
      "com.lightbend.akka" %% "akka-projection-durable-state" % AkkaProjectionVersion % Test
    val akkaProjectionTestKit = "com.lightbend.akka" %% "akka-projection-testkit" % AkkaProjectionVersion % Test

    val postgresql = "org.postgresql" % "postgresql" % "42.5.0" % Test

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.11" % Test // EPL 1.0 / LGPL 2.1
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.4" % Test // ApacheV2
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
    TestDeps.logback,
    TestDeps.scalaTest)

  val projection = Seq(
    akkaPersistenceQuery,
    r2dbcSpi,
    r2dbcPool,
    r2dbcPostgres,
    akkaProjectionCore,
    TestDeps.akkaProjectionEventSourced,
    TestDeps.akkaProjectionDurableState,
    TestDeps.akkaStreamTestkit,
    TestDeps.akkaTestkit,
    TestDeps.akkaProjectionTestKit,
    TestDeps.akkaJackson,
    TestDeps.logback,
    TestDeps.scalaTest)

  val migration =
    Seq(
      "com.lightbend.akka" %% "akka-persistence-jdbc" % "5.1.0" % Test,
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
