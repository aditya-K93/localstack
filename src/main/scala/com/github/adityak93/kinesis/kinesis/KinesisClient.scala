package com.github.adityak93.kinesis.kinesis

import cats.Parallel
import cats.effect.*
import cats.syntax.all.*
import com.github.adityak93.kinesis.Domain.*
import io.circe.Decoder
import io.circe.parser.parse
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import org.typelevel.log4cats.Logger
import retry.*
import retry.RetryPolicies.*
import retry.syntax.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryMode
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*

/**
 * Algebra for Kinesis operations.
 *
 * Category-theory lens: an algebra is a presentation of effects as morphisms in F.
 * The interpreter (live) is a natural transformation from this algebra into a concrete effect.
 */
trait KinesisClient[F[_]]:
  def listShards(streamName: String): F[List[String]]
  def getShardIterator(streamName: String, shardId: String, lastSequenceNumber: Option[String]): F[String]
  def getRecords(shardIterator: String): F[ProcessingResult]

object KinesisClient:

  object errors:

    def isInvalidSequence(e: Throwable): Boolean =
      val msg = Option(e.getMessage).getOrElse("")
      msg.contains("Record for provided SequenceNumber not found")

    def isConnectionClosed(e: Throwable): Boolean =
      val msg = Option(e.getMessage).getOrElse("")
      msg.contains("The connection was closed")

  end errors

  private def retryPolicy[F[_]: Temporal]: RetryPolicy[F, Throwable] = exponentialBackoff(100.milliseconds)
    .join(limitRetries[F](15))

  private def logRetry[F[_]](err: Throwable, details: RetryDetails)(using L: Logger[F]): F[Unit] =
    val msg = Option(err.getMessage).getOrElse(err.getClass.getName)
    details.nextStepIfUnsuccessful match
      case RetryDetails.NextStep.GiveUp                   => L
          .error(s"giving up after retries: retries=${details.retriesSoFar} cumulativeDelaySeconds=${details
              .cumulativeDelay.toSeconds} error=$msg")
      case RetryDetails.NextStep.DelayAndRetry(nextDelay) => L
          .warn(s"operation failed; retrying: retries=${details.retriesSoFar} nextDelayMillis=${nextDelay
              .toMillis} cumulativeDelaySeconds=${details.cumulativeDelay.toSeconds} error=$msg")

  private def parseEvent[F[_]](
    json: String
  )(using S: Sync[F], decoder: Decoder[Event], L: Logger[F]): F[Option[Event]] = S
    .fromEither(parse(json).flatMap(_.as[Event])).map(_.some).handleErrorWith { e =>
      val msg = Option(e.getMessage).getOrElse(e.getClass.getName)
      L.warn(s"failed to decode record; dropping: error=$msg") *> S.pure(none[Event])
    }

  def live[F[_]](
    client: KinesisAsyncClient
  )(using F: Async[F], P: Parallel[F], L: Logger[F], decoder: Decoder[Event]): KinesisClient[F] = new KinesisClient[F]:

    def listShards(streamName: String): F[List[String]] =
      val op =
        for
          _      <- L.info(s"listing shards: stream=$streamName")
          shards <- F.fromCompletableFuture(F.delay(
                      client.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
                        .thenApply(_.streamDescription().shards().asScala.map(_.shardId()).toList)
                    ))
          _      <- L.info(s"listed shards: stream=$streamName shards=${shards.size}")
        yield shards

      op.retryingOnErrors(
        policy = retryPolicy,
        errorHandler = ResultHandler.retryOnAllErrors[F, List[String]](logRetry)
      )

    private def buildShardIteratorRequest(
      streamName: String,
      shardId: String,
      lastSequenceNumber: Option[String]
    ): GetShardIteratorRequest =
      val b = GetShardIteratorRequest.builder().streamName(streamName).shardId(shardId)

      lastSequenceNumber match
        case None      => b.shardIteratorType(ShardIteratorType.LATEST).build()
        case Some(seq) => b.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(seq)
            .build()

    private def fetchIterator(streamName: String, shardId: String, lastSequenceNumber: Option[String]): F[String] = F
      .fromCompletableFuture(F.delay(
        client.getShardIterator(buildShardIteratorRequest(streamName, shardId, lastSequenceNumber))
          .thenApply(_.shardIterator())
      ))

    def getShardIterator(streamName: String, shardId: String, lastSequenceNumber: Option[String]): F[String] =
      val hasSeq = lastSequenceNumber.isDefined

      val op =
        for
          _  <- L.info(s"getting shard iterator: stream=$streamName shard=$shardId hasSequence=$hasSeq")
          it <-
            fetchIterator(streamName, shardId, lastSequenceNumber).handleErrorWith { e =>
              if errors.isInvalidSequence(e) then
                L.warn(s"sequence no longer valid; resetting iterator to LATEST: stream=$streamName shard=$shardId") *>
                  fetchIterator(streamName, shardId, None)
              else F.raiseError(e)
            }
          _  <- L.info(s"got shard iterator: stream=$streamName shard=$shardId")
        yield it

      op.retryingOnErrors(policy = retryPolicy, errorHandler = ResultHandler.retryOnAllErrors[F, String](logRetry))

    def getRecords(shardIterator: String): F[ProcessingResult] =
      for
        response  <- F.fromCompletableFuture(F.delay(
                       client.getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).limit(10000).build())
                     ))
        records    = response.records().asScala.toList
        events    <- records.parTraverse { record =>
                       val data = StandardCharsets.UTF_8.decode(record.data().asByteBuffer()).toString
                       parseEvent[F](data)
                     }.map(_.flatten)
        lastSeqNum = records.lastOption.map(_.sequenceNumber())
      yield ProcessingResult(events, response.nextShardIterator(), lastSeqNum)

  /**
   * Live resource for AWS Kinesis client.
   *
   * We keep this separate from the algebra interpreter because the SDK client has a lifecycle.
   */
  final case class AwsSettings(
    region: Region,
    endpoint: URI,
    maxConcurrency: Int,
    maxPendingConnectionAcquires: Int,
    connectionTimeout: Duration,
    readTimeout: Duration,
    writeTimeout: Duration,
    connectionAcquisitionTimeout: Duration
  )

  object AwsSettings:

    val localstackDefault: AwsSettings = AwsSettings(
      region = Region.US_EAST_1,
      endpoint = URI.create("http://localhost:4566"),
      maxConcurrency = 10000,
      maxPendingConnectionAcquires = 50000,
      connectionTimeout = Duration.ofSeconds(1),
      readTimeout = Duration.ofSeconds(10),
      writeTimeout = Duration.ofSeconds(10),
      connectionAcquisitionTimeout = Duration.ofSeconds(2)
    )

  def resource[F[_]: Async: Parallel](
    settings: AwsSettings
  )(using L: Logger[F], decoder: Decoder[Event]): Resource[F, KinesisClient[F]] = Resource
    .fromAutoCloseable(Async[F].delay {
      KinesisAsyncClient.builder().region(settings.region).endpointOverride(settings.endpoint)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))).httpClient(
          NettyNioAsyncHttpClient.builder().maxConcurrency(settings.maxConcurrency)
            .maxPendingConnectionAcquires(settings.maxPendingConnectionAcquires)
            .connectionTimeout(settings.connectionTimeout).readTimeout(settings.readTimeout)
            .writeTimeout(settings.writeTimeout).connectionAcquisitionTimeout(settings.connectionAcquisitionTimeout)
            .build()
        ).overrideConfiguration(ClientOverrideConfiguration.builder().retryStrategy(RetryMode.ADAPTIVE_V2).build())
        .build()
    }).map(live[F])
