package com.github.adityak93.kinesis

import cats.{Applicative, Parallel}
import cats.effect.{Async, Concurrent, ExitCode, IO, IOApp, Ref, Resource, Spawn, Temporal}
import cats.syntax.all.*
import fs2.{text, Stream}
import fs2.concurrent.Channel
import fs2.io.file.{Files, Flags, Path}
import retry.*
import retry.RetryPolicies.*
import retry.syntax.all.*
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  DescribeStreamRequest,
  GetRecordsRequest,
  GetRecordsResponse,
  GetShardIteratorRequest,
  ShardIteratorType
}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import org.typelevel.log4cats.Logger
import io.circe.parser.parse
import io.circe.generic.auto.*
import software.amazon.awssdk.core.retry.RetryMode

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

object KinesisApp:
  final case class Config(
      streamName: String,
      outputFile: String,
      maxConcurrency: Int = Runtime.getRuntime.availableProcessors() * 2
  )

  final case class Event(value: Int)
  final case class RunningStats(count: Long, sum: Long)
  final case class ProcessingResult(
      events: List[Event],
      nextIterator: String,
      lastSequenceNumber: Option[String]
  )

  trait KinesisClient[F[_]]:
    def listShards(streamName: String): F[List[String]]
    def getShardIterator(
        streamName: String,
        shardId: String,
        lastSequenceNumber: Option[String]
    ): F[String]
    def getRecords(shardIterator: String): F[ProcessingResult]

  object KinesisClient:
    private def retryPolicy[F[_]: Applicative]: RetryPolicy[F] =
      exponentialBackoff(100.milliseconds).join(
        limitRetries[F](15)
      )

    private def logError[F[_]](err: Throwable, details: RetryDetails)(using
        L: Logger[F]
    ): F[Unit] =
      details match
        case RetryDetails.GivingUp(totalRetries, totalDelay) =>
          L.error(
            s"Giving up after $totalRetries retries and ${totalDelay.toSeconds} seconds. Error: ${err.getMessage}"
          )
        case RetryDetails.WillDelayAndRetry(
              nextDelay,
              retriesSoFar,
              cumulativeDelay
            ) =>
          L.warn(
            s"Failed with ${err.getMessage}. Retried $retriesSoFar times. Next retry in ${nextDelay.toMillis}ms after ${cumulativeDelay.toSeconds}s total delay"
          )

    def live[F[_]](
        client: KinesisAsyncClient
    )(using F: Async[F], P: Parallel[F], L: Logger[F]): KinesisClient[F] =
      new KinesisClient[F]:
        def listShards(streamName: String): F[List[String]] =
          (for {
            _ <- L.info(s"Listing shards for stream $streamName")
            shards <- F.fromCompletableFuture(
              F.delay(
                client
                  .describeStream(
                    DescribeStreamRequest
                      .builder()
                      .streamName(streamName)
                      .build()
                  )
                  .thenApply(
                    _.streamDescription()
                      .shards()
                      .asScala
                      .map(_.shardId())
                      .toList
                  )
              )
            )
            _ <- L.info(s"Found ${shards.size} shards")
          } yield shards)
            .retryingOnAllErrors(policy = retryPolicy, onError = logError)

        private def tryGetIterator(streamName: String, shardId: String, seqNum: Option[String]): F[String] =
          F.fromCompletableFuture(
            F.delay(
              client
                .getShardIterator(
                  GetShardIteratorRequest
                    .builder()
                    .streamName(streamName)
                    .shardId(shardId)
                    .shardIteratorType(
                      seqNum.fold(
                        ShardIteratorType.TRIM_HORIZON
                      )(_ => ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    )
                    .startingSequenceNumber(seqNum.orNull)
                    .build()
                )
                .thenApply(_.shardIterator())
            )
          )

        def getShardIterator(
            streamName: String,
            shardId: String,
            lastSequenceNumber: Option[String]
        ): F[String] =

          for {
            _ <- L.info(
              s"Getting iterator for shard $shardId${lastSequenceNumber
                  .fold("")(seq => s" after sequence $seq")}"
            )
            iterator <- tryGetIterator(streamName, shardId, lastSequenceNumber)
              .handleErrorWith {
                case e
                    if e.getMessage.contains(
                      "Record for provided SequenceNumber not found"
                    ) =>
                  L.warn(
                    s"Sequence number no longer valid, resetting to LATEST"
                  ) *>
                    tryGetIterator(streamName, shardId, None) // Try again with no sequence number
                case e =>
                  F.raiseError(e) // Let other errors be handled by retry policy
              }
              .retryingOnAllErrors(policy = retryPolicy, onError = logError)
            _ <- L.info(s"Retrieved iterator for shard $shardId")
          } yield iterator

        def getRecords(shardIterator: String): F[ProcessingResult] =
          for {
            response <- F
              .fromCompletableFuture(
                F.delay(
                  client.getRecords(
                    GetRecordsRequest
                      .builder()
                      .shardIterator(shardIterator)
                      .limit(10000)
                      .build()
                  )
                )
              )
              .recoverWith { case error =>
                if (
                  error.getMessage.contains("The connection was closed") ||
                  error.getMessage.contains("Record for provided SequenceNumber not found")
                ) {
                  L.warn(
                    s"Connection error or invalid sequence detected for iterator $shardIterator, will retry from last sequence in loop"
                  ) *>
                    Async[F].raiseError(error) // Escalate to loop
                } else {
                  Async[F].raiseError(error) // Other errors escalate too
                }
              }
            records = response.records().asScala.toList
            events <- records.parTraverse { record =>
              val data = StandardCharsets.UTF_8
                .decode(record.data().asByteBuffer())
                .toString
              F.fromEither(parse(data).flatMap(_.as[Event]))
                .handleErrorWith(err =>
                  Logger[F].error(s"Failed to parse record: $err") *>
                    F.pure(Event(0))
                )
            }
            lastSeqNum = records.lastOption.map(_.sequenceNumber())
          } yield ProcessingResult(
            events,
            response.nextShardIterator(),
            lastSeqNum
          ) // Removed .retryingOnAllErrors

    def resource[F[_]: Async: Parallel: Logger]: Resource[F, KinesisClient[F]] =
      Resource
        .fromAutoCloseable(
          summon[Async[F]].delay {
            KinesisAsyncClient
              .builder()
              .region(Region.US_EAST_1)
              .endpointOverride(URI.create("http://localhost:4566"))
              .credentialsProvider(
                StaticCredentialsProvider.create(
                  AwsBasicCredentials.create("test", "test")
                )
              )
              .httpClient(
                NettyNioAsyncHttpClient
                  .builder()
                  .maxConcurrency(10000)
                  .maxPendingConnectionAcquires(50000)
                  .connectionTimeout(Duration.ofSeconds(1))
                  .readTimeout(Duration.ofSeconds(10))
                  .writeTimeout(Duration.ofSeconds(10))
                  .connectionAcquisitionTimeout(Duration.ofSeconds(2))
                  .build()
              )
              .overrideConfiguration(
                ClientOverrideConfiguration
                  .builder()
                  .retryStrategy(
                    RetryMode.ADAPTIVE_V2
                  )
                  .build()
              )
              .build()
          }
        )
        .map(live[F])

  def kinesisStream[F[_]: Async: Logger: Concurrent: Temporal](
      client: KinesisClient[F],
      config: Config,
      stats: Ref[F, RunningStats],
      errorChannel: Channel[F, String]
  ): Stream[F, Unit] = {
    def makeProcessShard(shardId: String): Stream[F, Unit] = {
      def loop(
          iterator: String,
          lastSequenceNumber: Option[String]
      ): Stream[F, Unit] =
        Stream
          .eval(client.getRecords(iterator))
          .flatMap { case ProcessingResult(events, nextIterator, newLastSeqNum) =>
            Stream.eval {
              stats.update { current =>
                RunningStats(
                  current.count + events.size,
                  current.sum + events.map(_.value).sum
                )
              } *>
                Logger[F]
                  .debug(
                    s"Processed ${events.size} records from shard $shardId"
                  )
                  .whenA(events.nonEmpty)
            } ++
              Option(nextIterator)
                .fold(
                  Stream.eval(
                    Logger[F].warn(
                      s"No next iterator for shard $shardId, restarting from last sequence $newLastSeqNum"
                    ) *>
                      Temporal[F].sleep(5.seconds)
                  ) *>
                    Stream
                      .eval(
                        client.getShardIterator(
                          config.streamName,
                          shardId,
                          newLastSeqNum // Use last sequence number to resume
                        )
                      )
                      .flatMap(newIter => loop(newIter, newLastSeqNum))
                )(iter => loop(iter, newLastSeqNum))
          }
          .handleErrorWith { error =>
            Stream.eval(
              errorChannel.send(error.getMessage) *>
                Logger[F].error(s"Error in shard $shardId: ${error.getMessage}")
            ) *>
              Stream.eval(Temporal[F].sleep(5.seconds)) *>
              Stream
                .eval(
                  Logger[F].warn(
                    s"Restarting shard $shardId from last sequence $lastSequenceNumber due to error"
                  ) *>
                    client.getShardIterator(config.streamName, shardId, lastSequenceNumber)
                )
                .flatMap(newIter => loop(newIter, lastSequenceNumber))
          }

      Stream
        .eval(client.getShardIterator(config.streamName, shardId, None))
        .flatMap(iterator => loop(iterator, None))
        .handleErrorWith { error =>
          Stream.eval(
            errorChannel.send(error.getMessage) *>
              Logger[F]
                .error(s"Fatal error in shard $shardId: ${error.getMessage}")
          ) *>
            Stream.eval(Temporal[F].sleep(10.seconds)) *>
            makeProcessShard(shardId)
        }
    }

    Stream
      .eval(client.listShards(config.streamName))
      .flatMap { shardIds =>
        Stream
          .emits(shardIds)
          .map(makeProcessShard)
          .parJoin(config.maxConcurrency)
      }
      .handleErrorWith { error =>
        Stream.eval(
          Logger[F].error(s"Stream error: ${error.getMessage}") *>
            Temporal[F].sleep(15.seconds)
        ) *> kinesisStream(client, config, stats, errorChannel)
      }
  }

  def writeSums[F[_]: Async: Files: Logger](
      outputFile: String,
      stats: Ref[F, RunningStats]
  ): Stream[F, Unit] = {
    val path = Path(outputFile)
    val headers = "Timestamp,Count,Sum\n"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

    Stream.eval(Files[F].exists(path)).flatMap { exists =>
      val headerStream =
        if (!exists)
          Stream
            .emit(headers)
            .through(text.utf8.encode)
            .through(Files[F].writeAll(path, flags = Flags.Append))
            .drain
        else
          Stream.emit(()).drain

      headerStream ++
        Stream
          .repeatEval(stats.get)
          .metered(10.seconds)
          .evalMap { currentStats =>
            val timestamp =
              LocalDateTime.now().format(formatter)
            Stream
              .emit(s"$timestamp,${currentStats.count},${currentStats.sum}\n")
              .through(text.utf8.encode)
              .through(
                Files[F].writeAll(Path(outputFile), flags = Flags.Append)
              )
              .compile
              .drain *> Logger[F].info(
              s"Wrote stats to file at $timestamp: $currentStats"
            )
          }
    }
  }

  def errorLogger[F[_]: Async: Logger](
      errorChannel: Channel[F, String]
  ): Stream[F, Unit] =
    errorChannel.stream.evalMap(msg => Logger[F].error(msg))

  def program[F[_]: Async: Files: Logger: Concurrent: Spawn: Parallel]: F[Unit] =
    (for {
      client <- KinesisClient.resource[F]
      stats <- Resource.eval(Ref[F].of(RunningStats(0L, 0L)))
      errorChannel <- Resource.eval(Channel.bounded[F, String](100))
      _ <- Resource.eval(Logger[F].info("Starting Kinesis consumer..."))
    } yield (client, stats, errorChannel)).use { case (client, stats, errorChannel) =>
      val config = Config(streamName = "my-stream", outputFile = "output.txt")
      Stream(
        kinesisStream(client, config, stats, errorChannel),
        errorLogger(errorChannel)
      ).parJoin(2)
        .concurrently(writeSums(config.outputFile, stats))
        .compile
        .drain <* Logger[F].info("Kinesis consumer stopped.")
    }.foreverM

object Main extends IOApp:
  private val logger = org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO]
  given org.typelevel.log4cats.Logger[IO] = logger

  def run(args: List[String]): IO[ExitCode] =
    KinesisApp
      .program[IO]
      .redeemWith(
        error => logger.error(error.getMessage) *> IO.pure(ExitCode.Error),
        _ => IO.pure(ExitCode.Success)
      )
