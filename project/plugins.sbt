addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0") // for maintenance of copyright file header
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")

// for dependency analysis
addDependencyTreePlugin

// for releasing
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

//// docs
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.4")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.57")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
