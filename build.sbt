ThisBuild / scalaVersion := "3.6.4"
ThisBuild / organization := "com.github.adityak93"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21"))
ThisBuild / githubWorkflowPublishTargetBranches := Seq()

lazy val root = (project in file("."))
  .settings(
    name := "kinesis-aggregator",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.6.1",
      "co.fs2" %% "fs2-core" % "3.12.0",
      "co.fs2" %% "fs2-io" % "3.12.0",
      "software.amazon.awssdk" % "kinesis" % "2.31.22",
      "io.circe" %% "circe-core" % "0.14.12",
      "io.circe" %% "circe-generic" % "0.14.12",
      "io.circe" %% "circe-parser" % "0.14.12",
      "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "ch.qos.logback" % "logback-classic" % "1.5.18",
      "com.github.cb372" %% "cats-retry" % "4.0.0",
      "org.typelevel" %% "cats-effect-testkit" % "3.6.1" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test,
      "org.scalacheck" %% "scalacheck" % "1.18.1" % Test,
      "org.typelevel" %% "scalacheck-effect" % "2.0-9366e44" % Test,
      "org.typelevel" %% "scalacheck-effect-munit" % "2.0-9366e44" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings"),
    fork := true,
    Compile / run / javaOptions += "-Dcats.effect.logNonFatalExceptions=false",
    Compile / run / javaOptions += "-Dcats.effect.warnOnNonMainThreadDetected=false"
  )
