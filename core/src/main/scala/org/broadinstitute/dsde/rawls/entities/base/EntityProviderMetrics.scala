package org.broadinstitute.dsde.rawls.entities.base

import nl.grons.metrics4.scala.{Counter, Timer}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented

trait EntityProviderMetrics extends RawlsInstrumented {

  private val FunctionKey = "function"
  private val ProviderNameKey = "providerName"

  private def entityProviderMetrics: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(WorkspaceDataMetricKey, "entityProvider")

  def requestLatency(functionName: String, providerName: String): Timer =
    entityProviderMetrics
      .expand(FunctionKey, functionName)
      .expand(ProviderNameKey, providerName)
      .asTimer("latency")

  def requestCount(functionName: String, providerName: String): Counter =
    entityProviderMetrics
      .expand(FunctionKey, functionName)
      .expand(ProviderNameKey, providerName)
      .asCounter("count")

  def errorCount(functionName: String, providerName: String, errorType: String): Counter =
    entityProviderMetrics
      .expand(FunctionKey, functionName)
      .expand(ProviderNameKey, providerName)
      .expand("errorType", errorType)
      .asCounter("errors")

}
