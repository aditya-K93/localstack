ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "com.github.adityak93"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / githubWorkflowJavaVersions += JavaSpec.temurin("17")
ThisBuild / githubWorkflowPublishTargetBranches := Seq()

lazy val root = (project in file("."))
  .settings(
    name := "kinesis-aggregator",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "co.fs2" %% "fs2-core" % "3.9.4",
      "co.fs2" %% "fs2-io" % "3.9.4",
      "software.amazon.awssdk" % "kinesis" % "2.20.155",
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",
      "org.typelevel" %% "log4cats-slf4j" % "2.6.0",
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings"),
    fork := true,
    Compile / run / javaOptions += "-Dcats.effect.logNonFatalExceptions=false"
  )
