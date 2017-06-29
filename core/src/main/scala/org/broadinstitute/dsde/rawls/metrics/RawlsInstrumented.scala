package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import nl.grons.metrics.scala._
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceName

import scala.annotation.implicitNotFound

/**
  * Mixin trait for instrumentation.
  * Extends metrics-scala [[DefaultInstrumented]] and provide additional utilties for generating
  * metric names for FireCloud.
  */
trait RawlsInstrumented extends DefaultInstrumented {
  // Keys for expanded metric fragments
  final val WorkspaceMetric = "workspace"
  final val SubmissionMetric = "submission"
  final val SubmissionStatusMetric = "submissionStatus"
  final val WorkflowStatusMetric = "workflowStatus"

  /**
    * Base name for all metrics. This will be prepended to all generated metric names.
    * Example: dev.firecloud.rawls
    */
  protected val rawlsMetricBaseName: String
  override lazy val metricBaseName = MetricName(rawlsMetricBaseName)

  /**
    * Typeclass for something that can be converted into a metric name fragment with a given key.
    * Metric name fragments are combined via ExpandedMetricBuilder to generate an "expanded" metric name.
    * By default this just calls toString on the object of type A, but this can be overridden.
    */
  @implicitNotFound(msg = "Cannot expand instances of type ${A}")
  protected sealed trait Expansion[A] {
    def makeName(key: String, a: A) = s"$key.${a.toString}"
  }

  // Typeclass instances:

  protected implicit object WorkspaceNameExpansion extends Expansion[WorkspaceName] {
    override def makeName(key: String, n: WorkspaceName): String = s"$key.${n.toString.replace('/', '.')}"
  }

  protected implicit object UUIDExpansion extends Expansion[UUID]

  protected implicit def WorkflowStatusExpansion[A <: WorkflowStatus] = new Expansion[A] {}

  protected implicit def SubmissionStatusExpansion[A <: SubmissionStatus] = new Expansion[A] {}

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
  protected class ExpandedMetricBuilder[A] private (m: String = "") {
    def expand[A: Expansion](key: String, a: A) = {
      new ExpandedMetricBuilder(
        (if (m == "") m else m + ".") + implicitly[Expansion[A]].makeName(key, a))
    }

    def asCounter(name: Option[String] = None): Counter =
      metrics.counter(makeName(name))

    def asGauge[T](name: Option[String] = None)(fn: => T): Gauge[T] =
      metrics.gauge(makeName(name))(fn)

    def asTimer(name: Option[String] = None): Timer = {
      metrics.timer(makeName(name))
    }

    private def makeName(name: Option[String]): String = {
      m + name.map(n => s".$n").getOrElse("")
    }
  }

  object ExpandedMetricBuilder {
    def expand[A: Expansion](key: String, a: A) = {
      new ExpandedMetricBuilder().expand(key, a)
    }
  }
}
