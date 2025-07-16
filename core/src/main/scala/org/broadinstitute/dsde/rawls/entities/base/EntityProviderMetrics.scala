package org.broadinstitute.dsde.rawls.entities.base

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.{DoubleHistogram, LongCounter}
import io.opentelemetry.instrumentation.api.semconv.http.HttpClientMetrics
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented

import scala.jdk.CollectionConverters._

trait EntityProviderMetrics extends RawlsInstrumented {

  protected val workbenchMetricBaseName: String

  private val FunctionKey = AttributeKey.stringKey("function")
  private val ProviderNameKey = AttributeKey.stringKey("providerName")
  private val ErrorClassKey = AttributeKey.stringKey("errorType")

  private val BucketBoundaries =
    List[java.lang.Double](0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0).asJava

  private def meter = GlobalOpenTelemetry.get().getMeter("RawlsMetrics")

  private def entityProviderFunctionLatency: DoubleHistogram =
    meter
      .histogramBuilder(s"${workbenchMetricBaseName}_provider_function_latency")
      .setDescription("Latency of entity provider function calls")
      .setUnit("ms")
      .setExplicitBucketBoundariesAdvice(BucketBoundaries)
      .build()

  private def entityProviderErrorCount: LongCounter =
    meter
      .counterBuilder(s"${workbenchMetricBaseName}_provider_error_count")
      .setDescription("Count of errors in entity provider functions")
      .setUnit("error")
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

}
