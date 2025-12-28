package com.github.adityak93.kinesis.app

import cats.effect.*
import cats.syntax.all.*
import com.github.adityak93.kinesis.Domain.Config

/**
 * Typed configuration loader.
 *
 * In the category of effects, this is the initial morphism that bootstraps the rest of the program.
 * We keep it separate so the rest of the code stays parametric in F and testable.
 */
object AppConfig:

  // Ciris is added as a dependency in build.sbt.
  import ciris.*

  def load[F[_]: Async]: F[Config] =
    val defaultMaxConc = Runtime.getRuntime.availableProcessors() * 2

    (
      env("KINESIS_STREAM_NAME").as[String].default("my-stream"),
      env("OUTPUT_FILE").as[String].default("output.txt"),
      env("MAX_CONCURRENCY").as[Int].default(defaultMaxConc),
      env("EMIT_FREQUENCY_SECONDS").as[Int].default(10)
    ).parMapN(Config.apply).load[F]
