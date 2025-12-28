package com.github.adityak93.kinesis.sink

import cats.effect.*
import cats.syntax.all.*
import com.github.adityak93.kinesis.Domain.RunningStats
import fs2.io.file.{ Files, Flags, Path }
import fs2.{ Stream, text }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.typelevel.log4cats.Logger
import scala.concurrent.duration.*

/**
 * Utility to write running statistics to a CSV file at regular intervals.
 */
object StatsWriter:

  def writeSums[F[_]: Async: Files](outputFile: String, stats: Ref[F, RunningStats], emitEvery: Int)(using
    L: Logger[F]
  ): Stream[F, Unit] =
    val path      = Path(outputFile)
    val headers   = "Timestamp,Count,Sum\n"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

    Stream.eval[F, Boolean](Files[F].exists(path)).flatMap { exists =>
      val ensureHeader: Stream[F, Unit] =
        if !exists then
          Stream.emit(headers).through(text.utf8.encode).through(Files[F].writeAll(path, flags = Flags.Append)).drain
        else Stream.empty

      ensureHeader ++ Stream.repeatEval(stats.get).metered(emitEvery.seconds).evalMap { currentStats =>
        val timestamp = LocalDateTime.now().format(formatter)
        Stream.emit(s"$timestamp,${currentStats.count},${currentStats.sum}\n").through(text.utf8.encode)
          .through(Files[F].writeAll(path, flags = Flags.Append)).compile.drain *>
          L.info(s"wrote running stats: outputFile=$outputFile timestamp=$timestamp count=${currentStats
              .count} sum=${currentStats.sum}")
      }
    }
