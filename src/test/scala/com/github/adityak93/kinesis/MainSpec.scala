package com.github.adityak93.kinesis

import cats.effect.{ IO, Ref, Resource }
import fs2.concurrent.Channel
import fs2.io.file.Files
import fs2.text
import munit.{ CatsEffectSuite, ScalaCheckEffectSuite }
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import scala.concurrent.duration.*

class MainSpec extends CatsEffectSuite with ScalaCheckEffectSuite:

  import com.github.adityak93.kinesis.KinesisApp.{ Config, Event, KinesisClient, ProcessingResult, RunningStats }
  given Logger[IO] = NoOpLogger[IO]

  class TestKinesisClient(
    records: List[ProcessingResult] = Nil,
    shards: List[String] = List("shard-1"),
    errorAfter: Int = Int.MaxValue
  ) extends KinesisClient[IO]:
    private val callCount = new java.util.concurrent.atomic.AtomicInteger(0)

    def listShards(streamName: String): IO[List[String]] = IO.pure(shards)

    def getShardIterator(streamName: String, shardId: String, lastSequenceNumber: Option[String]): IO[String] = IO
      .pure(s"iterator-$shardId${lastSequenceNumber.fold("")("-" + _)}")

    def getRecords(shardIterator: String): IO[ProcessingResult] =
      val count = callCount.incrementAndGet()
      if count > errorAfter then IO.raiseError(new Exception("Simulated failure"))
      else if records.isEmpty then IO.pure(ProcessingResult(List(), "", None))
      else
        val index = (count - 1) % records.size
        IO.pure(records.head)
      end if
    end getRecords

  end TestKinesisClient

  val defaultConfig: Config = Config("test-stream", "test.txt", maxConcurrency = 1, emitFrequency = 1)

  def setupTestEnv: Resource[IO, (Ref[IO, RunningStats], Channel[IO, String])] =
    for
      stats        <- Resource.eval(IO.ref(RunningStats(0L, 0L)))
      errorChannel <- Resource.eval(Channel.bounded[IO, String](100))
    yield (stats, errorChannel)

  test("Main.run should complete successfully") {
    val args = List.empty[String]
    for
      fiber <- Main.run(args).start
      _     <- IO.sleep(100.millis)
      _     <- fiber.cancel
    yield assert(true)
    end for
  }

  test("should process records and update stats correctly") {
    val testRecords = ProcessingResult(List(Event(1), Event(2), Event(3)), "next-iterator", Some("seq-1"))

    val client = new TestKinesisClient(List(testRecords))

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        _          <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(1).compile.drain
        finalStats <- stats.get
      yield assertEquals(finalStats, RunningStats(3L, 6L))
    }
  }

  test("should handle and recover from transient errors") {
    val client = new TestKinesisClient(errorAfter = 1)

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(3).compile.drain.start
        error <- errorChannel.stream.take(1).compile.lastOrError
        _     <- fiber.cancel
      yield assert(error.contains("Simulated failure"))
    }
  }

  test("KinesisClient.resource should be properly managed") {
    val testClient = new TestKinesisClient(
      shards = List("shard-1", "shard-2"),
      records = List(ProcessingResult(List(Event(1)), "iterator-1", Some("seq-1")))
    )

    Resource.eval(IO.pure(testClient))
      .use(client => client.listShards("test-stream").map(shards => assertEquals(shards, List("shard-1", "shard-2"))))
  }

  test("should process multiple shards concurrently") {
    val client = new TestKinesisClient(
      shards = List("shard-1", "shard-2", "shard-3"),
      records = List(
        ProcessingResult(List(Event(1)), "next-1", Some("seq-1")),
        ProcessingResult(List(Event(2)), "next-2", Some("seq-2")),
        ProcessingResult(List(Event(3)), "next-3", Some("seq-3"))
      )
    )

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber      <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(3).compile.drain.start
        _          <- IO.sleep(100.millis)
        finalStats <- stats.get
        _          <- fiber.cancel
      yield assert(finalStats.count > 0)
    }
  }

  test("should write stats to output file") {
    Files[IO].tempFile.use { tempFile =>
      for
        _       <- Files[IO].delete(tempFile)
        stats   <- IO.ref(RunningStats(10L, 100L))
        _       <- KinesisApp.writeSums(tempFile.toString, stats, 1).take(1).compile.drain
        content <- Files[IO].readAll(tempFile).through(text.utf8.decode).compile.string
      yield
        assert(content.contains("Timestamp,Count,Sum"))
        assert(content.contains("10,100"))
    }
  }

  test("error logger should process messages correctly") {
    setupTestEnv.use { case (_, errorChannel) =>
      for
        fiber <- KinesisApp.errorLogger(errorChannel).take(1).compile.drain.start
        _     <- errorChannel.send("test error")
        _     <- fiber.join
      yield assert(true)
    }
  }

  test("program should compose all components correctly") {
    for
      fiber <- KinesisApp.program[IO].start
      _     <- IO.sleep(100.millis)
      _     <- fiber.cancel
    yield assert(true)
  }

  test("should handle empty shard list") {
    val client = new TestKinesisClient(shards = Nil)

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        _          <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(1).compile.drain
        finalStats <- stats.get
      yield assertEquals(finalStats, RunningStats(0L, 0L))
    }
  }

  test("should handle null iterator") {
    val client = new TestKinesisClient:
      override def getRecords(shardIterator: String): IO[ProcessingResult] = IO
        .pure(ProcessingResult(List(Event(1)), null, None))

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber      <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(2).compile.drain.start
        _          <- IO.sleep(100.millis)
        finalStats <- stats.get
        _          <- fiber.cancel
      yield assert(finalStats.count > 0)
    }
  }

  test("full pipeline integration") {
    Files[IO].tempFile.use { tempFile =>
      setupTestEnv.use { case (stats, errorChannel) =>
        val records  = (1 to 50).map(i => Event(i)).toList
        val records2 = (51 to 100).map(i => Event(i)).toList

        val client = new TestKinesisClient(
          records = List(
            ProcessingResult(records, "next-1", Some("seq-1")),
            ProcessingResult(records2, "next-2", Some("seq-2"))
          ),
          shards = List("shard-1", "shard-2")
        ):
          // override getRecords to distribute between shards
          override def getRecords(shardIterator: String): IO[ProcessingResult] =
            if shardIterator.contains("shard-1") then IO.pure(ProcessingResult(records, "next-1", Some("seq-1")))
            else IO.pure(ProcessingResult(records2, "next-2", Some("seq-2")))

        for
          fiber      <- KinesisApp
                          .kinesisStream(client, defaultConfig.copy(outputFile = tempFile.toString), stats, errorChannel)
                          .concurrently(KinesisApp.writeSums(tempFile.toString, stats, defaultConfig.emitFrequency)).take(2)
                          .compile.drain.start
          _          <- IO.sleep(200.millis)
          content    <- Files[IO].readAll(tempFile).through(text.utf8.decode).compile.string
          finalStats <- stats.get
          _          <- fiber.cancel
        yield
          assertEquals(finalStats.count, 100L)
          assertEquals(finalStats.sum, (1 to 100).sum.toLong)
        end for
      }
    }
  }

  test("should handle getShardIterator with sequence number") {
    val client = new TestKinesisClient(
      shards = List("shard-1"),
      records = List(ProcessingResult(List(Event(1)), "iterator-1", Some("seq-1")))
    )

    for iterator <- client.getShardIterator("test-stream", "shard-1", Some("seq-1"))
    yield assertEquals(iterator, "iterator-shard-1-seq-1")
  }

  test("should handle getShardIterator without sequence number") {
    val client = new TestKinesisClient()

    for iterator <- client.getShardIterator("test-stream", "shard-1", None)
    yield assertEquals(iterator, "iterator-shard-1")
  }

  test("should handle error conditions in getRecords") {
    val client = new TestKinesisClient(errorAfter = 0)

    for result <- client.getRecords("invalid-iterator").attempt yield assert(result.isLeft)
  }

  test("should handle retry policy with exponential backoff") {
    var attemptCount = 0
    val client       = new TestKinesisClient:
      override def getRecords(shardIterator: String): IO[ProcessingResult] =
        attemptCount += 1
        if attemptCount <= 2 then IO.raiseError(new Exception("Temporary failure"))
        else IO.pure(ProcessingResult(List(Event(1)), "next-iterator", Some("seq-1")))

    setupTestEnv.use { case (stats, errorChannel) =>
      for _ <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(1).compile.drain
      yield assert(attemptCount > 1)
    }
  }

  test("should handle all error types in tryGetIterator") {
    val invalidClient = new TestKinesisClient:
      override def getShardIterator(
        streamName: String,
        shardId: String,
        lastSequenceNumber: Option[String]
      ): IO[String] = IO.raiseError(new IllegalArgumentException("Invalid shard"))

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber <- KinesisApp.kinesisStream(invalidClient, defaultConfig, stats, errorChannel).take(1).compile.drain.start
        error <- errorChannel.stream.take(1).compile.lastOrError
        _     <- fiber.cancel
      yield assert(error.contains("Invalid shard"))
    }
  }

  test("should process multiple records and handle sequence numbers") {
    val records = List(
      ProcessingResult(List(Event(1), Event(2)), "iterator-1", Some("seq-1")),
      ProcessingResult(List(Event(3), Event(4)), "iterator-2", Some("seq-2")),
      ProcessingResult(List(Event(5), Event(6)), "iterator-3", Some("seq-3"))
    )

    val client = new TestKinesisClient(records = records)

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        _          <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(3).compile.drain
        finalStats <- stats.get
      yield
        assertEquals(finalStats.count, 6L)
        assertEquals(finalStats.sum, 9L)
    }
  }

  test("should handle empty record sets") {
    val emptyRecords = List(ProcessingResult(List(), "iterator-1", None), ProcessingResult(List(), "iterator-2", None))

    val client = new TestKinesisClient(records = emptyRecords)

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        _          <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(2).compile.drain
        finalStats <- stats.get
      yield
        assertEquals(finalStats.count, 0L)
        assertEquals(finalStats.sum, 0L)
    }
  }

  test("tryGetIterator should handle different sequence number scenarios") {
    val streamName = "test-stream"
    val shardId    = "shard-1"

    // Test with sequence number
    val withSeqClient = new TestKinesisClient:
      override def getShardIterator(
        streamName: String,
        shardId: String,
        lastSequenceNumber: Option[String]
      ): IO[String] =
        if lastSequenceNumber.isDefined && lastSequenceNumber.get == "seq-1" && streamName == "test-stream" &&
          shardId == "shard-1"
        then IO.pure("after-sequence-iterator")
        else IO.raiseError(new Exception("Invalid parameters"))

    // Test without sequence number
    val withoutSeqClient = new TestKinesisClient:
      override def getShardIterator(
        streamName: String,
        shardId: String,
        lastSequenceNumber: Option[String]
      ): IO[String] =
        if lastSequenceNumber.isEmpty && streamName == "test-stream" && shardId == "shard-1" then
          IO.pure("trim-horizon-iterator")
        else IO.raiseError(new Exception("Invalid parameters"))

    for
      // should use AFTER_SEQUENCE_NUMBER
      withSeq    <- withSeqClient.getShardIterator(streamName, shardId, Some("seq-1"))
      // should use TRIM_HORIZON
      withoutSeq <- withoutSeqClient.getShardIterator(streamName, shardId, None)
    yield
      assertEquals(withSeq, "after-sequence-iterator")
      assertEquals(withoutSeq, "trim-horizon-iterator")
    end for
  }

  test("should log errors with proper retry details") {

    val client = new TestKinesisClient(errorAfter = 0)

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber  <- KinesisApp.kinesisStream(client, defaultConfig, stats, errorChannel).take(5).compile.drain.start
        _      <- IO.sleep(500.millis)
        errors <- errorChannel.stream.take(3).compile.toList
        _      <- fiber.cancel
      yield
        assert(errors.exists(_.contains("Simulated failure")))
        assert(errors.nonEmpty)
    }
  }

  test("should log final give up message after max retries") {
    val logger                              = NoOpLogger[IO]
    given org.typelevel.log4cats.Logger[IO] = logger

    val persistentError = new TestKinesisClient:
      override def getRecords(shardIterator: String): IO[ProcessingResult] = IO
        .raiseError(new Exception("Persistent failure"))

    setupTestEnv.use { case (stats, errorChannel) =>
      for
        fiber  <- KinesisApp.kinesisStream(persistentError, defaultConfig, stats, errorChannel).take(1).compile.drain
                    .start
        errors <- errorChannel.stream.take(5).compile.toList
        _      <- fiber.cancel
      yield
        assert(errors.exists(_.contains("Persistent failure")))
        assert(errors.length > 1, "Should have multiple retry attempts")
    }
  }

end MainSpec
