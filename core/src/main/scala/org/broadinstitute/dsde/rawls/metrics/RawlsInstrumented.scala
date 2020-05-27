package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import nl.grons.metrics.scala.Counter
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 7/13/17.
  */
trait RawlsInstrumented extends WorkbenchInstrumented {

  // Keys for expanded metric fragments
  final val SubmissionMetricKey        = "submission"
  final val SubmissionStatusMetricKey  = "submissionStatus"
  final val SubsystemMetricKey         = "subsystem"
  final val WorkflowStatusMetricKey    = "workflowStatus"
  final val WorkspaceMetricKey         = "workspace"

  /**
    * An ExpandedMetricBuilder for a WorkspaceName.
    */
  protected def workspaceMetricBuilder(workspaceName: WorkspaceName): ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(WorkspaceMetricKey, workspaceName)

  /**
    * An ExpandedMetricBuilder for a WorkspaceName and a submission ID.
    */
  protected def workspaceSubmissionMetricBuilder(workspaceName: WorkspaceName, submissionId: UUID): ExpandedMetricBuilder =
    workspaceMetricBuilder(workspaceName).expand(SubmissionMetricKey, submissionId)

  /**
    * Provides a counter for a SubmissionStatus.
    * @param builder base builder used to generate the counter
    * @return SubmissionStatus => Counter
    */
  protected def submissionStatusCounter(builder: ExpandedMetricBuilder): SubmissionStatus => Counter =
    status => builder
      .expand(SubmissionStatusMetricKey, status)
      .transient()
      .asCounter("count")

  /**
    * Provides a counter for a WorkflowStatus.
    * @param builder base builder used to generate the counter
    * @return WorkflowStatus => Counter
    */
  protected def workflowStatusCounter(builder: ExpandedMetricBuilder): WorkflowStatus => Counter =
    status => builder
      .expand(WorkflowStatusMetricKey, status)
      .transient()
      .asCounter("count")
}

object RawlsInstrumented {
  /**
    * Adds a .countDBResult method to Counter which counts the result of a numeric DBIOAction.
    */
  implicit class CounterDBIOActionSupport(counter: Counter) {
    def countDBResult[R, S <: NoStream, E <: Effect](action: DBIOAction[R, S, E])(implicit numeric: Numeric[R], executionContext: ExecutionContext): DBIOAction[R, NoStream, E] =
      action.map { count =>
        counter += numeric.toLong(count)
        count
      }
  }
}