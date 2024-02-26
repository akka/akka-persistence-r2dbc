name := "native-image-tests"

version := "1.0"

scalaVersion := "2.13.12"

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val akkaVersion = sys.props.getOrElse("akka.version", "2.9.1")
lazy val akkaR2dbcVersion = sys.props.getOrElse("akka.r2dbc.version", "1.2.2")

// Run in a separate JVM, to make sure sbt waits until all threads have
// finished before returning.
// If you want to keep the application running while executing other
// sbt tasks, consider https://github.com/spray/sbt-revolver/
fork := true

// GraalVM native image build
enablePlugins(NativeImagePlugin)
nativeImageJvm := "graalvm-community"
nativeImageVersion := "21.0.2"
nativeImageOptions := Seq(
  "--no-fallback",
  "--verbose",
  "--initialize-at-build-time=ch.qos.logback",
  "-Dakka.native-image.debug=true")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "com.lightbend.akka" %% "akka-persistence-r2dbc" % akkaR2dbcVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.13",
  // H2
  "com.h2database" % "h2" % "2.2.224",
  "io.r2dbc" % "r2dbc-h2" % "1.0.0.RELEASE")
