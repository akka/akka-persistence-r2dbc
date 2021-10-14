import sbt.Keys.parallelExecution
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

GlobalScope / parallelExecution := false
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

inThisBuild(
  Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://doc.akka.io/docs/akka-persistence-r2dbc/current")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/akka/akka-persistence-r2dbc"),
        "https://github.com/akka/akka-persistence-r2dbc.git")),
    startYear := Some(2021),
    developers += Developer(
      "contributors",
      "Contributors",
      "https://gitter.im/akka/dev",
      url("https://github.com/akka/akka-persistence-r2dbc/graphs/contributors")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    description := "An Akka Persistence backed by SQL database with R2DBC",
    // add snapshot repo when Akka version overriden
    resolvers ++=
      (if (System.getProperty("override.akka.version") != null)
         Seq("Akka Snapshots".at("https://oss.sonatype.org/content/repositories/snapshots/"))
       else Seq.empty)))

def common: Seq[Setting[_]] =
  Seq(
    crossScalaVersions := Seq(Dependencies.Scala213),
    scalaVersion := Dependencies.Scala213,
    crossVersion := CrossVersion.binary,
    scalafmtOnCompile := true,
    sonatypeProfileName := "com.lightbend",
    // Setting javac options in common allows IntelliJ IDEA to import them automatically
    Compile / javacOptions ++= Seq("-encoding", "UTF-8", "-source", "1.8", "-target", "1.8"),
    headerLicense := Some(HeaderLicense.Custom("""Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>""")),
    Test / logBuffered := false,
    Test / parallelExecution := false,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    Test / fork := true, // some non-heap memory is leaking
    Test / javaOptions ++= Seq("-Xms1G", "-Xmx1G", "-XX:MaxDirectMemorySize=256M"),
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value))

lazy val dontPublish = Seq(skip in publish := true, publishArtifact in Compile := false)

lazy val root = (project in file("."))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-r2dbc-root",
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
  .aggregate(core, projection)

def suffixFileFilter(suffix: String): FileFilter = new SimpleFileFilter(f => f.getAbsolutePath.endsWith(suffix))

lazy val core = (project in file("core"))
  .settings(common)
  .settings(name := "akka-persistence-r2dbc", libraryDependencies ++= Dependencies.core)

lazy val projection = (project in file("projection"))
  .dependsOn(core)
  .settings(common)
  .settings(name := "akka-projection-r2dbc", libraryDependencies ++= Dependencies.projection)

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PublishRsyncPlugin)
  .dependsOn(core)
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "Akka Persistence R2DBC",
    crossScalaVersions := Seq(Dependencies.Scala212),
    previewPath := (Paradox / siteSubdirName).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-r2dbc/${projectInfoVersion.value}",
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    Compile / paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/docs/akka-persistence-r2dbc/current/",
      "canonical.base_url" -> "https://doc.akka.io/docs/akka-persistence-r2dbc/current",
      "akka.version" -> Dependencies.AkkaVersion,
      "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
      "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
      "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/"),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io")
