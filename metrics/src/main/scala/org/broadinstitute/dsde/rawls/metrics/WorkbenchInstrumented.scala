package org.broadinstitute.dsde.rawls.metrics

import com.codahale.metrics.{Gauge => DropwizardGauge}
import nl.grons.metrics.scala._
import org.broadinstitute.dsde.rawls.metrics.Expansion._

import scala.collection.JavaConverters._
import spray.http.{HttpRequest, HttpResponse, Uri}

/**
  * Mixin trait for instrumentation.
  * Extends metrics-scala [[DefaultInstrumented]] and provides additional utilties for generating
  * metric names for Workbench.
  */
trait WorkbenchInstrumented extends DefaultInstrumented {
  /**
    * Base name for all metrics. This will be prepended to all generated metric names.
    * Example: dev.firecloud.rawls
    */
  protected val workbenchMetricBaseName: String
  override lazy val metricBaseName = MetricName(workbenchMetricBaseName)

  /**
    * Utility for building expanded metric names in a typesafe way. Example usage:
    * {{{
    *   val counter: Counter =
    *     ExpandedMetricBuilder
    *       .expand(WorkspaceMetric, workspaceName)
    *       .expand(SubmissionMetric, submissionId)
    *       .expand(WorkflowStatusMetric, status)
    *       .asCounter("count")
    *   // counter has name:
    *   // <baseName>.workspace.<workspaceNamespace>.<workspaceName>.submission.<submissionId>.workflowStatus.<workflowStatus>.count
    *   counter += 1000
    * }}}
    *
    * Note the above will only compile if there are [[Expansion]] instances for the types passed to the expand method.
    */
  protected class ExpandedMetricBuilder private (m: String = "") {
    def expand[A: Expansion](key: String, a: A): ExpandedMetricBuilder = {
      new ExpandedMetricBuilder(
        (if (m == "") m else m + ".") + implicitly[Expansion[A]].makeNameWithKey(key, a))
    }

    def getFullName(name: String): String =
      metricBaseName.append(makeName(name)).name

    def asCounter(name: String): Counter =
      metrics.counter(makeName(name))

    def asGauge[T](name: String)(fn: => T): Gauge[T] =
      metrics.gauge(makeName(name))(fn)

    def asGaugeIfAbsent[T](name: String)(fn: => T): Gauge[T] = {
      // Get the fully qualified metric name for inspecting the registry.
      val gaugeName = getFullName(name)
      metricRegistry.getGauges().asScala.get(gaugeName) match {
        case None =>
          // If the gauge does not exist in the registry, create it
          asGauge[T](name)(fn)
        case Some(gauge) =>
          // If the gauge exists in the registry, return it.
          // Need to wrap the returned Java DropwizardGauge in a Scala Gauge.
          new Gauge[T](gauge.asInstanceOf[DropwizardGauge[T]])
      }
    }

    def asTimer(name: String): Timer =
      metrics.timer(makeName(name))

    def asHistogram(name: String): Histogram =
      metrics.histogram(makeName(name))

    def unregisterMetric(name: String): Boolean = {
      val metricName = getFullName(name)
      metricRegistry.remove(metricName)
    }

    private def makeName(name: String): String =
      if (m.nonEmpty) s"$m.$name" else name

    override def toString: String = m
  }

  object ExpandedMetricBuilder {
    def expand[A: Expansion](key: String, a: A): ExpandedMetricBuilder = {
      new ExpandedMetricBuilder().expand(key, a)
    }

    def empty: ExpandedMetricBuilder = {
      new ExpandedMetricBuilder()
    }
  }

  // Keys for expanded metric fragments
  final val HttpRequestMethodMetricKey      = "httpRequestMethod"
  final val HttpRequestUriMetricKey         = "httpRequestUri"
  final val HttpResponseStatusCodeMetricKey = "httpResponseStatusCode"

  // Handy definitions which can be used by implementing classes:

  protected def httpRequestMetricBuilder(builder: ExpandedMetricBuilder): (HttpRequest, HttpResponse) => ExpandedMetricBuilder = {
    (httpRequest, httpResponse) => builder
      .expand(HttpRequestMethodMetricKey, httpRequest.method)
      .expand(HttpRequestUriMetricKey, httpRequest.uri)(UriExpansion)
      .expand(HttpResponseStatusCodeMetricKey, httpResponse.status)
  }

  protected implicit def httpRequestCounter(implicit builder: ExpandedMetricBuilder): (HttpRequest, HttpResponse) => Counter =
    httpRequestMetricBuilder(builder)(_, _).asCounter("request")

  protected implicit def httpRequestTimer(implicit builder: ExpandedMetricBuilder): (HttpRequest, HttpResponse) => Timer =
    httpRequestMetricBuilder(builder)(_, _).asTimer("latency")

  protected implicit def httpRetryHistogram(implicit builder: ExpandedMetricBuilder): Histogram =
    builder.asHistogram("retry")

  // Let subclasses override the UriExpansion if desired

  protected val UriExpansion: Expansion[Uri] = implicitly
}
