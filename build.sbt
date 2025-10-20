ThisBuild / scalaVersion := "3.7.3"
ThisBuild / organization := "com.github.adityak93"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21"))
ThisBuild / githubWorkflowPublishTargetBranches := Seq()
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val root = (project in file("."))
    .settings(
      name := "kinesis-aggregator",
      libraryDependencies ++= Seq(
        "com.github.ghik" % "zerowaste" % "1.0.0" cross CrossVersion.full,
        "org.typelevel" %% "cats-effect" % "3.6.3",
        "co.fs2" %% "fs2-core" % "3.12.2",
        "co.fs2" %% "fs2-io" % "3.12.2",
        "software.amazon.awssdk" % "kinesis" % "2.33.13",
        "software.amazon.awssdk" % "sqs" % "2.33.13",
        "software.amazon.awssdk" % "netty-nio-client" % "2.33.13",
        "io.circe" %% "circe-core" % "0.14.12",
        "io.circe" %% "circe-generic" % "0.14.12",
        "io.circe" %% "circe-parser" % "0.14.12",
        "org.typelevel" %% "log4cats-slf4j" % "2.7.0",
        "org.slf4j" % "slf4j-api" % "2.0.17",
        "com.github.cb372" %% "cats-retry" % "4.0.0",
        "ch.qos.logback" % "logback-classic" % "1.5.19",
        "io.netty" % "netty-codec-http2" % "4.2.5.Final",
        "io.netty" % "netty-transport" % "4.2.5.Final",
        "io.netty" % "netty-handler" % "4.1.125.Final",
        "com.thesamet.scalapb" %% "scalapb-runtime" % "1.0.0-alpha.2",
        "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "1.0.0-alpha.2",
        "org.typelevel" %% "cats-effect-testkit" % "3.6.1" % Test,
        "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test,
        "org.scalacheck" %% "scalacheck" % "1.18.1" % Test,
        "org.typelevel" %% "scalacheck-effect" % "2.0-9366e44" % Test,
        "org.typelevel" %% "scalacheck-effect-munit" % "2.0-9366e44" % Test
      ),
      scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings"),
      fork := true,
      Compile / run / javaOptions += "-Dcats.effect.logNonFatalExceptions=false",
      Compile / run / javaOptions += "-Dcats.effect.warnOnNonMainThreadDetected=false",
      Compile / PB.targets := Seq(
        scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
      )
    )
