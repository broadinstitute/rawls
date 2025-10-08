package org.broadinstitute.dsde.rawls.entities.base

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.{DoubleHistogram, LongCounter, LongHistogram}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented

import java.util.stream.DoubleStream
import scala.jdk.CollectionConverters._

trait EntityProviderMetrics extends RawlsInstrumented {

  private val PREFIX = "rawls_entityprovider"

  private val FunctionKey = AttributeKey.stringKey("function")
  private val ProviderNameKey = AttributeKey.stringKey("providername")
  private val ErrorClassKey = AttributeKey.stringKey("errortype")

  // every 10ms up to 100ms, then every 190ms up to 2s (190 ends neatly at 2s), then every 2s up to
  // 10s, then every 20s up to 9m
  private val BucketBoundaries: java.util.List[
    java.lang.Double
  ] = // does all the math in terms of milliseconds, then converts to seconds, otherwise the
    // precision is wonky
    DoubleStream
      .iterate(10,
               (d: Double) => d < 60000 * 9,
               (d: Double) => {
                 def foo(d: Double): Double =
                   if (d < 100) {
                     d + 10
                   } else if (d < 2000) {
                     d + 190
                   } else if (d < 10000) {
                     d + 2000
                   } else
                     d + 20000

                 foo(d)
               }
      )
      .map((d: Double) => d / 1000.0)
      .boxed
      .toList

  private def meter = GlobalOpenTelemetry.get().getMeter("RawlsMetrics")

  private def entityProviderFunctionLatency: DoubleHistogram =
    meter
      .histogramBuilder(s"${PREFIX}_function_latency")
      .setDescription("Latency of entity provider function calls")
      .setUnit("ms")
      .setExplicitBucketBoundariesAdvice(BucketBoundaries)
      .build()

  private def entityProviderErrorCount: LongCounter =
    meter
      .counterBuilder(s"${PREFIX}_error_count")
      .setDescription("Count of errors in entity provider functions")
      .setUnit("error")
      .build()

  private def sortMemoryRetryAttempts: LongHistogram =
    meter
      .histogramBuilder(s"${PREFIX}_sortmemretry_retries")
      .ofLongs()
      // this counts the number of query attempts, so we can be pretty sure of the bucket boundaries
      .setExplicitBucketBoundariesAdvice(java.util.List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .setDescription("Number of sort-memory retries required to complete a query")
      .setUnit("retries")
      .build()

  // bucket boundaries are powers of 2, starting at 2Mb and ending with 512Mb
  private val allocationBuckets: List[java.lang.Long] =
    List(1, 2, 4, 8, 16, 32, 64, 128, 256).map(multiplier => 2 * 1024 * 1024 * multiplier)
  private def sortMemoryRetryAllocation: LongHistogram =
    meter
      .histogramBuilder(s"${PREFIX}_sortmemretry_allocation")
      .ofLongs()
      .setExplicitBucketBoundariesAdvice(allocationBuckets.asJava)
      .setDescription("Sort memory allocation required to complete a query")
      .setUnit("bytes")
      .build()

  private def nameOf(provider: EntityProvider): String = provider.getClass.getSimpleName

  private def nameOf(error: Throwable): String = error.getClass.getSimpleName

  def recordFunctionLatency(functionName: String, provider: EntityProvider, durationMs: Double): Unit = {
    val attrs = Attributes.of(
      FunctionKey,
      functionName,
      ProviderNameKey,
      nameOf(provider)
    )
    entityProviderFunctionLatency.record(durationMs, attrs)
  }

  def recordError(functionName: String, provider: EntityProvider, error: Throwable): Unit = {
    val attrs = Attributes.of(
      FunctionKey,
      functionName,
      ProviderNameKey,
      nameOf(provider),
      ErrorClassKey,
      nameOf(error)
    )
    entityProviderErrorCount.add(1, attrs)
  }

  def recordSortMemoryRetryResult(functionName: String, numRetries: Long, byteAllocation: Long): Unit = {
    val attrs = Attributes.of(
      FunctionKey,
      functionName
    )
    sortMemoryRetryAttempts.record(numRetries, attrs)
    sortMemoryRetryAllocation.record(byteAllocation, attrs)
  }
}
