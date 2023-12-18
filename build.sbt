import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts
import com.typesafe.tools.mima.plugin.MimaKeys.mimaReportSignatureProblems
import sbt.Keys.parallelExecution
import com.geirsson.CiReleasePlugin

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
    releaseNotesURL := (
      if (isSnapshot.value) None
      else Some(url(s"https://github.com/akka/akka-persistence-r2dbc/releases/tag/v${version.value}"))
    ),
    licenses := Seq(("BUSL-1.1", url("https://raw.githubusercontent.com/akka/akka-persistence-r2dbc/main/LICENSE"))),
    description := "An Akka Persistence backed by SQL database with R2DBC",
    // append -SNAPSHOT to version when isSnapshot
    dynverSonatypeSnapshots := true,
    resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
    // add snapshot repo when Akka version overriden
    resolvers ++=
      (if (System.getProperty("override.akka.version") != null)
         Seq("Akka library snapshot repository".at("https://repo.akka.io/snapshots"))
       else Seq.empty)))

def common: Seq[Setting[_]] =
  Seq(
    crossScalaVersions := Dependencies.Scala2Versions,
    scalaVersion := Dependencies.Scala213,
    crossVersion := CrossVersion.binary,
    scalafmtOnCompile := true,
    // Setting javac options in common allows IntelliJ IDEA to import them automatically
    Compile / javacOptions ++= Seq("-encoding", "UTF-8", "--release", "11"),
    Compile / scalacOptions ++= Seq("-release", "11"),
    headerLicense := Some(
      HeaderLicense.Custom("""Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>""")),
    Test / logBuffered := false,
    Test / parallelExecution := false,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    Test / fork := true, // some non-heap memory is leaking
    Test / javaOptions ++= {
      import scala.collection.JavaConverters._
      // include all passed -Dakka. properties to the javaOptions for forked tests
      // useful to switch DB dialects for example
      val akkaProperties = System.getProperties.stringPropertyNames.asScala.toList.collect {
        case key: String if key.startsWith("akka.") || key.startsWith("conf") =>
          "-D" + key + "=" + System.getProperty(key)
      }
      "-Xms1G" :: "-Xmx1G" :: "-XX:MaxDirectMemorySize=256M" :: akkaProperties
    },
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    Global / excludeLintKeys += projectInfoVersion,
    Global / excludeLintKeys += mimaReportSignatureProblems,
    Global / excludeLintKeys += mimaPreviousArtifacts,
    mimaReportSignatureProblems := true,
    mimaPreviousArtifacts :=
      Set(
        organization.value %% moduleName.value % previousStableVersion.value
          .getOrElse(throw new Error("Unable to determine previous version"))))

lazy val dontPublish = Seq(publish / skip := true, Compile / publishArtifact := false)

lazy val root = (project in file("."))
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "akka-persistence-r2dbc-root",
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin, MimaPlugin, CiReleasePlugin)
  .aggregate(core, migration, migrationTests, docs)

def suffixFileFilter(suffix: String): FileFilter = new SimpleFileFilter(f => f.getAbsolutePath.endsWith(suffix))

lazy val core = (project in file("core"))
  .settings(common)
  .settings(Scala3.settings)
  .settings(name := "akka-persistence-r2dbc", libraryDependencies ++= Dependencies.core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(CiReleasePlugin)

lazy val migration = (project in file("migration"))
  .settings(common)
  .settings(
    name := "akka-persistence-r2dbc-migration",
    Test / mainClass := Some("akka.persistence.r2dbc.migration.MigrationTool"),
    Test / run / fork := true,
    Test / run / javaOptions ++= {
      import scala.collection.JavaConverters._
      // include all passed -Dakka. properties to the javaOptions for forked tests
      // useful to switch DB dialects for example
      val akkaProperties = System.getProperties.stringPropertyNames.asScala.toList.collect {
        case key: String if key.startsWith("akka.") => "-D" + key + "=" + System.getProperty(key)
      }
      "-Dlogback.configurationFile=logback-main.xml" :: "-Xms1G" :: "-Xmx1G" :: "-XX:MaxDirectMemorySize=256M" :: akkaProperties
    })
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)

lazy val migrationTests = (project in file("migration-tests"))
  .settings(common)
  .settings(name := "akka-persistence-r2dbc-migration-tests", libraryDependencies ++= Dependencies.migrationTests)
  .dependsOn(migration)
  .dependsOn(core % "compile->compile;test->test")
  .enablePlugins(AutomateHeaderPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .settings(dontPublish)

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .dependsOn(core, migration)
  .settings(common)
  .settings(dontPublish)
  .settings(
    name := "Akka Persistence plugin for R2DBC",
    libraryDependencies ++= Dependencies.docs,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-r2dbc/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-r2dbc/${projectInfoVersion.value}",
    paradoxGroups := Map(
      "Language" -> Seq("Java", "Scala"),
      "Dialect" -> Seq("SQL Server", "Postgres", "Yugabyte", "H2")),
    Compile / paradoxProperties ++= Map(
      "project.url" -> "https://doc.akka.io/docs/akka-persistence-r2dbc/current/",
      "canonical.base_url" -> "https://doc.akka.io/docs/akka-persistence-r2dbc/current",
      "akka.version" -> Dependencies.AkkaVersion,
      "h2.version" -> Dependencies.H2Version,
      "r2dbc-h2.version" -> Dependencies.R2dbcH2Version,
      "scala.version" -> scalaVersion.value,
      "scala.binary.version" -> scalaBinaryVersion.value,
      "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
      "extref.akka-docs.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
      "extref.akka-projection.base_url" -> s"https://doc.akka.io/docs/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/%s",
      "extref.java-docs.base_url" -> "https://docs.oracle.com/en/java/javase/11/%s",
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/current/",
      "scaladoc.akka.persistence.r2dbc.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
      "javadoc.akka.persistence.r2dbc.base_url" -> "", // no Javadoc is published
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersionInDocs}/",
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersionInDocs}/",
      "scaladoc.com.typesafe.config.base_url" -> s"https://lightbend.github.io/config/latest/api/"),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io")

val isJdk11orHigher: Boolean = {
  val result = VersionNumber(sys.props("java.specification.version")).matchesSemVer(SemanticSelector(">=11"))
  if (!result)
    throw new IllegalArgumentException("JDK 11 or higher is required")
  result
}
