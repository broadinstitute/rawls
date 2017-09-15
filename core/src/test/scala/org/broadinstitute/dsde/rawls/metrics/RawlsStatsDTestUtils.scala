package org.broadinstitute.dsde.rawls.metrics

import java.util.UUID

import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{Submission, Workspace, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.concurrent.Eventually

/**
  * Created by rtitle on 7/14/17.
  */
trait RawlsStatsDTestUtils extends StatsDTestUtils { this: Eventually with MockitoTestUtils =>

  protected def expectedHttpRequestMetrics(method: String, path: String, statusCode: Int, expectedTimes: Int, subsystem: Option[Subsystem] = None): Set[(String, String)] = {
    val prefix = s"test.${subsystem.map(s => s"subsystem.${s.toString}.").getOrElse("")}httpRequestMethod.$method.httpRequestUri.$path.httpResponseStatusCode.$statusCode"
    val expectedTimesStr = expectedTimes.toString
    Set(
      (s"$prefix.request", expectedTimesStr),
      (s"$prefix.latency.samples", expectedTimesStr)
    )
  }

  protected def expectedWorkflowStatusMetric(workspaceName: WorkspaceName, submissionId: String, workflowStatus: WorkflowStatus, expectedTimes: Int): (String, String) =
    (s"${workbenchMetricBaseName}.workspace.${workspaceName.toString.replace('/', '.')}.submission.$submissionId.workflowStatus.${workflowStatus.toString}.count", expectedTimes.toString)

  protected def expectedWorkflowStatusMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus, expectedTimes: Int): (String, String) =
    expectedWorkflowStatusMetric(workspace.toWorkspaceName, submission.submissionId, workflowStatus, expectedTimes)

  protected def expectedWorkflowStatusMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus): (String, String) =
    expectedWorkflowStatusMetric(workspace, submission, workflowStatus, submission.workflows.size)

  protected def expectedSubmissionStatusMetric(workspace: Workspace, submissionStatus: SubmissionStatus, expectedTimes: Int = 1): (String, String) =
    expectedSubmissionStatusMetric(workspace.toWorkspaceName, submissionStatus, expectedTimes)

  protected def expectedSubmissionStatusMetric(workspaceName: WorkspaceName, submissionStatus: SubmissionStatus, expectedTimes: Int): (String, String) =
    (s"${workbenchMetricBaseName}.workspace.${workspaceName.toString.replace('/', '.')}.submissionStatus.${submissionStatus.toString}.count", expectedTimes.toString)
}
