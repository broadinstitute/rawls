package org.broadinstitute.dsde.rawls.metrics

import nl.grons.metrics.scala._
import org.broadinstitute.dsde.rawls.metrics.Expansion._
import org.broadinstitute.dsde.rawls.model.WorkspaceName

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

    def asCounter(name: String): Counter =
      metrics.counter(makeName(name))

    def asGauge[T](name: String)(fn: => T): Gauge[T] =
      metrics.gauge(makeName(name))(fn)

    def asTimer(name: String): Timer =
      metrics.timer(makeName(name))

    private def makeName(name: String): String = s"$m.$name"

    override def toString: String = m
  }

  object ExpandedMetricBuilder {
    def expand[A: Expansion](key: String, a: A): ExpandedMetricBuilder = {
      new ExpandedMetricBuilder().expand(key, a)
    }

    def empty: ExpandedMetricBuilder = new ExpandedMetricBuilder()
  }

  // Keys for expanded metric fragments
  final val WorkspaceMetricKey  = "workspace"

  // Handy definitions which can be used by implementing classes:

  /**
    * An ExpandedMetricBuilder for a WorkspaceName.
    */
  protected def workspaceMetricBuilder(workspaceName: WorkspaceName): ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(WorkspaceMetricKey, workspaceName)
}
