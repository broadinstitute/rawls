package org.broadinstitute.dsde.rawls.entities.base

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.{DoubleHistogram, LongCounter, LongHistogram}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented

import scala.jdk.CollectionConverters._

trait EntityProviderMetrics extends RawlsInstrumented {

  private val PREFIX = "rawls_entityprovider"

  private val FunctionKey = AttributeKey.stringKey("function")
  private val ProviderNameKey = AttributeKey.stringKey("providername")
  private val ErrorClassKey = AttributeKey.stringKey("errortype")

  private val BucketBoundaries =
    List[java.lang.Double](0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0).asJava

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
