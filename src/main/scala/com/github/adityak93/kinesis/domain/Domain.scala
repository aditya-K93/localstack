package com.github.adityak93.kinesis

import io.circe.Decoder
import io.circe.generic.semiauto.*
import scala.concurrent.duration.FiniteDuration

/**
 * Domain model (pure data).
 *
 * In categorical terms: these are objects in the category of Scala values (types),
 * free of effects; all side-effects are pushed to interpreters living in F.
 */
object Domain:

  final case class Config(
    streamName: String,
    outputFile: String,
    maxConcurrency: Int = Runtime.getRuntime.availableProcessors() * 2,
    emitFrequency: Int = 10
  )

  final case class Event(value: Int)

  object Event:
    given Decoder[Event] = deriveDecoder[Event]

  final case class RunningStats(count: Long, sum: Long)

  /**
   * Result of a single GetRecords call.
   *
   * Note: nextIterator may be null in tests / Java SDK boundaries; treat with Option(nextIterator).
   */
  final case class ProcessingResult(events: List[Event], nextIterator: String, lastSequenceNumber: Option[String])
