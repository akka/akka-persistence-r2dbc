/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {
  val Scala212 = "2.12.14"
  val Scala213 = "2.13.1"
  val AkkaVersion = System.getProperty("override.akka.version", "2.6.16")
  val AkkaVersionInDocs = AkkaVersion.take(3)
  // for example
  val AkkaHttpVersion = "10.2.6"
  val AkkaManagementVersion = "1.0.6"
  val R2dbcVersion = "0.9.0.M2"
  val AkkaProjectionVersion = "1.2.2"

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion
    val akkaClusterTyped = "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.6" // EPL 1.0 / LGPL 2.1

    // FIXME remove when EventSourcedProvider2 has been incorporated in Akka Projection
    val akkaProjectionEventSourced = "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion
    val akkaProjectionCore = "com.lightbend.akka" %% "akka-projection-core" % AkkaProjectionVersion

    val postgresql = "org.postgresql" % "postgresql" % "42.2.24"
//    val r2dbcSpi = "io.r2dbc" % "r2dbc-spi" % R2dbcVersion
//    val r2dbcPool = "io.r2dbc" % "r2dbc-pool" % R2dbcVersion
//    val r2dbcPostgres = "org.postgresql" % "r2dbc-postgresql" % R2dbcVersion

    val r2dbcSpi = "io.r2dbc" % "r2dbc-spi" % "0.8.6.RELEASE"
    val r2dbcPool = "io.r2dbc" % "r2dbc-pool" % "0.8.7.RELEASE"
    val r2dbcPostgres = "io.r2dbc" % "r2dbc-postgresql" % "0.8.10.RELEASE"
  }

  object TestDeps {
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test
    val akkaTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
    val akkaJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion % Test

    val akkaProjectionTestKit = "com.lightbend.akka" %% "akka-projection-testkit" % AkkaProjectionVersion % Test

    val logback = Compile.logback % Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1" % Test // ApacheV2
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
    postgresql,
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
    postgresql,
    akkaProjectionEventSourced,
    akkaProjectionCore,
    TestDeps.akkaStreamTestkit,
    TestDeps.akkaTestkit,
    TestDeps.akkaProjectionTestKit,
    TestDeps.akkaJackson,
    TestDeps.logback,
    TestDeps.scalaTest)
}
