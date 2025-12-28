package com.github.adityak93.kinesis.stream

import cats.effect.*
import cats.syntax.all.*
import com.github.adityak93.kinesis.Domain.*
import com.github.adityak93.kinesis.kinesis.KinesisClient
import fs2.Stream
import fs2.concurrent.Channel
import org.typelevel.log4cats.Logger
import scala.concurrent.duration.*

/**
 * FS2 stream that consumes all shards in parallel.
 *
 * Think of this as a coalgebra (unfold) producing an infinite stream of effects,
 * where failure is handled by restarting the shard loop from a checkpoint.
 */
object KinesisConsumer:

  def stream[F[_]: Temporal](
    client: KinesisClient[F],
    config: Config,
    stats: Ref[F, RunningStats],
    errorChannel: Channel[F, String]
  )(using L: Logger[F]): Stream[F, Unit] =

    def processShard(shardId: String): Stream[F, Unit] =

      def restartFrom(lastSequenceNumber: Option[String]): Stream[F, Unit] = Stream
        .eval[F, String](L.warn(s"restarting shard from last checkpoint: stream=${config
            .streamName} shard=$shardId hasSequence=${lastSequenceNumber
            .isDefined}") *> client.getShardIterator(config.streamName, shardId, lastSequenceNumber))
        .flatMap(it => loop(it, lastSequenceNumber))

      def loop(iterator: String, lastSequenceNumber: Option[String]): Stream[F, Unit] = Stream
        .eval[F, ProcessingResult](client.getRecords(iterator))
        .flatMap { case ProcessingResult(events, nextIterator, newLastSeqNum) =>
          val updateStats = stats.update { current =>
            val deltaCount = events.size.toLong
            val deltaSum   = events.iterator.map(_.value.toLong).sum
            RunningStats(current.count + deltaCount, current.sum + deltaSum)
          }

          val logProcessed = L
            .debug(s"processed records: stream=${config.streamName} shard=$shardId records=${events.size}")
            .whenA(events.nonEmpty)

          val continue = Option(nextIterator) match
            case Some(it) if it.nonEmpty => loop(it, newLastSeqNum)
            case _                       => Stream.eval(L.warn(s"missing next iterator; reacquiring: stream=${config
                  .streamName} shard=$shardId") *> Temporal[F].sleep(5.seconds)) *> restartFrom(newLastSeqNum)

          Stream.eval[F, Unit](updateStats *> logProcessed) ++ continue
        }.handleErrorWith { e =>
          val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
          Stream.eval[F, Unit](errorChannel.send(msg) *> L.error(s"shard loop failed: stream=${config
              .streamName} shard=$shardId error=$msg")) *> Stream.eval(Temporal[F].sleep(5.seconds)) *>
            restartFrom(lastSequenceNumber)
        }

      Stream.eval[F, String](client.getShardIterator(config.streamName, shardId, None)).flatMap(it => loop(it, None))
        .handleErrorWith { e =>
          val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
          Stream.eval[F, Unit](errorChannel.send(msg) *> L.error(s"fatal shard failure; restarting: stream=${config
              .streamName} shard=$shardId error=$msg")) *> Stream.eval(Temporal[F].sleep(10.seconds)) *>
            processShard(shardId)
        }

    Stream.eval[F, List[String]](client.listShards(config.streamName))
      .flatMap(shardIds => Stream.emits(shardIds).map(processShard).parJoin(config.maxConcurrency)).handleErrorWith {
        e =>
          val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
          Stream.eval[F, Unit](L.error(s"consumer stream failed; restarting: stream=${config
              .streamName} error=$msg") *> Temporal[F].sleep(15.seconds)) *> stream(client, config, stats, errorChannel)
      }
