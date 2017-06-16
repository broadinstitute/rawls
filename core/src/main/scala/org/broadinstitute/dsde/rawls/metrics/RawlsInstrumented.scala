package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import nl.grons.metrics.scala._
import org.broadinstitute.dsde.rawls.model.{SubmissionStatuses, WorkflowStatuses, WorkspaceName}

/**
  * Created by rtitle on 6/15/17.
  */
trait RawlsInstrumented extends DefaultInstrumented {
  final val Workspace = "workspace"
  final val Submission = "submission"
  final val SubmissionStatus = "submissionStatus"
  final val WorkflowStatus = "workflowStatus"

  val rawlsMetricBaseName: String

  override lazy val metricBaseName = MetricName(rawlsMetricBaseName)

  sealed trait Expansion[A] {
    def makeName(key: String, a: A) = s"$key.${a.toString}"
  }

  implicit object WorkspaceNameExpansion extends Expansion[WorkspaceName] {
    override def makeName(key: String, n: WorkspaceName): String = s"$key.${n.toString.replace('/', '.')}"
  }

  implicit object UUIDExpansion extends Expansion[UUID]

  implicit object WorkflowStatusExpansion extends Expansion[WorkflowStatuses.WorkflowStatus]

  implicit object SubmissionStatusExpansion extends Expansion[SubmissionStatuses.SubmissionStatus]


  protected class ExpandedMetricBuilder[A] private (m: String = "") {
    def expand[A: Expansion](key: String, a: A) = {
      new ExpandedMetricBuilder(m + "." + implicitly[Expansion[A]].makeName(key, a))
    }

    def asCounter(name: Option[String] = None): Counter =
      metrics.counter(makeName(name))

    def asGauge[T](name: Option[String] = None)(fn: => T): Gauge[T] =
      metrics.gauge(makeName(name))(fn)

    def asTimer(name: Option[String] = None): Timer = {
      metrics.timer(makeName(name))
    }

    private def makeName(name: Option[String]): String =
      metricBaseName.name + "." + m + name.map(n => s".$n").getOrElse("")
  }

  object ExpandedMetricBuilder {
    def expand[A: Expansion](key: String, a: A) = {
      new ExpandedMetricBuilder().expand(key, a)
    }
  }
}
