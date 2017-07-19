package org.broadinstitute.dsde.rawls.metrics

import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{Submission, Workspace, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.concurrent.Eventually

/**
  * Created by rtitle on 7/14/17.
  */
trait RawlsStatsDTestUtils extends StatsDTestUtils { this: Eventually with MockitoTestUtils =>

  protected def expectedWorkflowStatusMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus, expectedTimes: Int): (String, String) =
    (s"${workbenchMetricBaseName}.workspace.${workspace.toWorkspaceName.toString.replace('/', '.')}.submission.${submission.submissionId}.workflowStatus.${workflowStatus.toString}.count", expectedTimes.toString)

  protected def expectedWorkflowStatusMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus): (String, String) =
    expectedWorkflowStatusMetric(workspace, submission, workflowStatus, submission.workflows.size)

  protected def expectedSubmissionStatusMetric(workspace: Workspace, submissionStatus: SubmissionStatus, expectedTimes: Int = 1): (String, String) =
    expectedSubmissionStatusMetric(workspace.toWorkspaceName, submissionStatus, expectedTimes)

  protected def expectedSubmissionStatusMetric(workspaceName: WorkspaceName, submissionStatus: SubmissionStatus, expectedTimes: Int): (String, String) =
    (s"${workbenchMetricBaseName}.workspace.${workspaceName.toString.replace('/', '.')}.submissionStatus.${submissionStatus.toString}.count", expectedTimes.toString)

}
