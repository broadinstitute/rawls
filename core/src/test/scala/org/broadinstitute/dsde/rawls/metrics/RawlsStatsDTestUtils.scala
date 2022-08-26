package org.broadinstitute.dsde.rawls.metrics

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

  protected def expectedHttpRequestMetrics(method: String,
                                           path: String,
                                           statusCode: Int,
                                           expectedTimes: Int,
                                           subsystem: Option[Subsystem] = None
  ): Set[(String, String)] = {
    val prefix =
      s"test.${subsystem.map(s => s"subsystem.${s.toString}.").getOrElse("")}httpRequestMethod.$method.httpRequestUri.$path.httpResponseStatusCode.$statusCode"
    val expectedTimesStr = expectedTimes.toString
    Set(
      (s"$prefix.request", expectedTimesStr),
      (s"$prefix.latency.samples", expectedTimesStr)
    )
  }

  private def expectedWorkflowStatusBase(workspaceName: WorkspaceName,
                                         submissionId: String,
                                         workflowStatus: WorkflowStatus
  ): String =
    s"${workbenchMetricBaseName}.transient.workspace.${workspaceName.toString.replace('/', '.')}.submission.$submissionId.workflowStatus.${workflowStatus.toString}"

  protected def expectedWorkflowStatusMetric(workspaceName: WorkspaceName,
                                             submissionId: String,
                                             workflowStatus: WorkflowStatus,
                                             expectedTimes: Int
  ): (String, String) =
    (s"${expectedWorkflowStatusBase(workspaceName, submissionId, workflowStatus)}.count", expectedTimes.toString)

  protected def expectedWorkflowStatusGauge(workspaceName: WorkspaceName,
                                            submissionId: String,
                                            workflowStatus: WorkflowStatus,
                                            expectedRate: Int
  ): (String, String) =
    (s"${expectedWorkflowStatusBase(workspaceName, submissionId, workflowStatus)}.current", expectedRate.toString)

  protected def expectedWorkflowStatusMetric(workspace: Workspace,
                                             submission: Submission,
                                             workflowStatus: WorkflowStatus,
                                             expectedTimes: Int
  ): (String, String) =
    expectedWorkflowStatusMetric(workspace.toWorkspaceName, submission.submissionId, workflowStatus, expectedTimes)

  protected def expectedWorkflowStatusMetric(workspace: Workspace,
                                             submission: Submission,
                                             workflowStatus: WorkflowStatus
  ): (String, String) =
    expectedWorkflowStatusMetric(workspace, submission, workflowStatus, submission.workflows.size)

  private def expectedSubmissionStatusBase(workspaceName: WorkspaceName, submissionStatus: SubmissionStatus): String =
    s"${workbenchMetricBaseName}.transient.workspace.${workspaceName.toString.replace('/', '.')}.submissionStatus.${submissionStatus.toString}"

  protected def expectedSubmissionStatusMetric(workspace: Workspace,
                                               submissionStatus: SubmissionStatus,
                                               expectedTimes: Int = 1
  ): (String, String) =
    expectedSubmissionStatusMetric(workspace.toWorkspaceName, submissionStatus, expectedTimes)

  protected def expectedSubmissionStatusMetric(workspaceName: WorkspaceName,
                                               submissionStatus: SubmissionStatus,
                                               expectedTimes: Int
  ): (String, String) =
    (s"${expectedSubmissionStatusBase(workspaceName, submissionStatus)}.count", expectedTimes.toString)

  protected def expectedSubmissionStatusGauge(workspaceName: WorkspaceName,
                                              submissionStatus: SubmissionStatus,
                                              expectedRate: Int
  ): (String, String) =
    (s"${expectedSubmissionStatusBase(workspaceName, submissionStatus)}.current", expectedRate.toString)

  protected def expectedGlobalWorkflowStatusGauge(workflowStatus: WorkflowStatus, expectedRate: Int): (String, String) =
    (s"${workbenchMetricBaseName}.workflowStatus.${workflowStatus.toString}.current", expectedRate.toString)

  protected def expectedGlobalSubmissionStatusGauge(submissionStatus: SubmissionStatus,
                                                    expectedRate: Int
  ): (String, String) =
    (s"${workbenchMetricBaseName}.submissionStatus.${submissionStatus.toString}.current", expectedRate.toString)
}
