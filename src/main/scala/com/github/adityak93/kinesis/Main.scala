package com.github.adityak93.kinesis

import cats.effect.*
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/**
 * Main entry point for the Kinesis Aggregator application.
 */
object Main extends IOApp:

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] = KinesisApp.program[IO].as(ExitCode.Success).handleErrorWith { e =>
    val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
    summon[Logger[IO]].error(s"fatal program error: error=$msg") *> IO.pure(ExitCode.Error)
  }
