package com.github.adityak93.kinesis

import cats.Parallel
import cats.effect.*
import cats.syntax.all.*
import com.github.adityak93.kinesis.app.AppConfig
import com.github.adityak93.kinesis.kinesis.KinesisClient as LiveKinesisClient
import com.github.adityak93.kinesis.sink.StatsWriter
import com.github.adityak93.kinesis.stream.KinesisConsumer
import fs2.Stream
import fs2.concurrent.Channel
import fs2.io.file.Files
import org.typelevel.log4cats.Logger

/**
 * Application façade kept for API stability.
 *
 * We preserve the original `KinesisApp.*` entrypoints used by tests,
 * while the implementation lives in smaller, composable modules.
 */
object KinesisApp:

  // ---- Public domain API (stable) -------------------------------------------

  export Domain.{ Config, Event, ProcessingResult, RunningStats }
  export com.github.adityak93.kinesis.kinesis.KinesisClient

  // ---- Public functions (stable) -------------------------------------------

  def kinesisStream[F[_]: Temporal](
    client: KinesisClient[F],
    config: Config,
    stats: Ref[F, RunningStats],
    errorChannel: Channel[F, String]
  )(using L: Logger[F]): Stream[F, Unit] = KinesisConsumer.stream(client, config, stats, errorChannel)

  def writeSums[F[_]: Async: Files](outputFile: String, stats: Ref[F, RunningStats], emitEvery: Int)(using
    L: Logger[F]
  ): Stream[F, Unit] = StatsWriter.writeSums(outputFile, stats, emitEvery)

  def errorLogger[F[_]: Async](errorChannel: Channel[F, String])(using L: Logger[F]): Stream[F, Unit] = errorChannel
    .stream.evalMap { msg =>
      val m = Option(msg).getOrElse("<null>")
      L.error(s"error channel: error=$m")
    }

  /**
   * Full program wiring.
   *
   * Category theory view: this is where we pick concrete interpreters (Resources)
   * for our abstract algebras, yielding a closed program in IO.
   */
  def program[F[_]: Async: Files: Parallel](using L: Logger[F]): F[Unit] =
    val mk: Resource[F, (KinesisClient[F], Ref[F, RunningStats], Channel[F, String], Config)] =
      for
        config <- Resource.eval(AppConfig.load[F])
        _      <- Resource.eval(L.info(s"starting consumer: stream=${config.streamName} outputFile=${config.outputFile}"))
        client <- LiveKinesisClient.resource[F](LiveKinesisClient.AwsSettings.localstackDefault)
        stats  <- Resource.eval(Ref[F].of(RunningStats(0L, 0L)))
        errs   <- Resource.eval(Channel.bounded[F, String](100))
      yield (client, stats, errs, config)

    mk.use { case (client, stats, errs, config) =>
      Stream(kinesisStream(client, config, stats, errs), errorLogger(errs)).parJoin(2)
        .concurrently(writeSums(config.outputFile, stats, config.emitFrequency)).compile.drain <*
        L.info("consumer stopped")
    }.foreverM
