package com.github.adityak93.kinesis
import cats.effect.{Async, Resource, IO, ExitCode}
import scala.jdk.CollectionConverters.*
import cats.syntax.all.*
import fs2.{Stream, text}
import fs2.io.file.{Files, Path}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{
  GetRecordsRequest,
  GetShardIteratorRequest,
  ShardIteratorType
}
import io.circe.parser.*
import io.circe.generic.auto.*
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*
import java.net.URI
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object KinesisApp {

  final case class Config(
      streamName: String,
      shardId: String,
      outputFile: String
  )

  final case class Event(value: Int)

  trait KinesisClient[F[_]]:
    def getShardIterator(streamName: String, shardId: String): F[String]
    def getRecords(shardIterator: String): F[(List[Event], String)]

  object KinesisClient:
    def live[F[_]](client: KinesisAsyncClient)(using
        F: Async[F]
    ): KinesisClient[F] = new KinesisClient[F]:
      def getShardIterator(streamName: String, shardId: String): F[String] =
        F.fromCompletableFuture(
          F.delay(
            client
              .getShardIterator(
                GetShardIteratorRequest
                  .builder()
                  .streamName(streamName)
                  .shardId(shardId)
                  .shardIteratorType(ShardIteratorType.LATEST)
                  .build()
              )
              .thenApply(_.shardIterator())
          )
        )

      def getRecords(shardIterator: String): F[(List[Event], String)] =
        F.fromCompletableFuture(
          F.delay(
            client.getRecords(
              GetRecordsRequest
                .builder()
                .shardIterator(shardIterator)
                .build()
            )
          )
        ).map { response =>
          val events = response
            .records()
            .asScala
            .map { record =>
              val data = StandardCharsets.UTF_8
                .decode(record.data().asByteBuffer())
                .toString
              parse(data).flatMap(_.as[Event]).toOption
            }
            .toList
            .flatten
          (events, response.nextShardIterator())
        }

    def resource[F[_]: Async: Logger]: Resource[F, KinesisClient[F]] =
      Resource
        .fromAutoCloseable(
          summon[Async[F]].delay(
            KinesisAsyncClient
              .builder()
              .region(software.amazon.awssdk.regions.Region.US_EAST_1)
              .endpointOverride(URI.create("http://localhost:4566"))
              .credentialsProvider(
                StaticCredentialsProvider
                  .create(AwsBasicCredentials.create("test", "test"))
              )
              .build()
          )
        )
        .map(live[F])

  def kinesisStream[F[_]: Async: Logger](
      client: KinesisClient[F],
      config: Config
  ): Stream[F, Event] =
    Stream
      .eval(client.getShardIterator(config.streamName, config.shardId))
      .flatMap { initialIterator =>
        Stream
          .unfoldEval(initialIterator) { iterator =>
            client.getRecords(iterator).map { case (events, nextIterator) =>
              Some((events, nextIterator))
            }
          }
          .flatMap(Stream.emits)
      }
      .metered(1.second)
      .handleErrorWith { error =>
        Stream.eval(
          Logger[F]
            .error(error)(s"Failed in Kinesis stream: ${error.getMessage}")
        ) *>
          Stream.empty
      }

  def aggregateEvents[F[_]: Logger]: Stream[F, Event] => Stream[F, Int] =
    _.scan(0)((acc, event) => acc + event.value)
      .evalTap(sum => Logger[F].info(s"Appending aggregated sum to file: $sum"))

  def writeToFile[F[_]: Async: Files](
      outputFile: String
  ): Stream[F, Int] => Stream[F, Unit] =
    _.map(_.toString)
      .intersperse("\n")
      .through(text.utf8.encode)
      .through(Files[F].writeAll(Path(outputFile)))

  def program[F[_]: Async: Files: Logger](
      client: KinesisClient[F],
      config: Config
  ): F[Unit] =
    kinesisStream(client, config)
      .through(aggregateEvents)
      .through(writeToFile(config.outputFile))
      .compile
      .drain
}

object Main extends cats.effect.IOApp {
  val config = KinesisApp.Config(
    streamName = "my-stream",
    shardId = "shardId-000000000000",
    outputFile = "output.txt"
  )
  given Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    KinesisApp.KinesisClient
      .resource[IO]
      .use { client =>
        KinesisApp.program(client, config).as(ExitCode.Success)
      }
      .redeemWith(
        error => Logger[IO].error(error.getMessage).as(ExitCode.Error),
        _ => IO.pure(ExitCode.Success)
      )
}
