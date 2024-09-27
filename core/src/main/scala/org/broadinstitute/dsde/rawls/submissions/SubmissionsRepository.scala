package org.broadinstitute.dsde.rawls.submissions

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{WorkflowStatuses, Workspace, WorkspaceName}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Data access for workflow submissions
  *
  * The intention of this class is to hide direct dependencies on Slick behind a relatively clean interface
  * to ease testability of higher level business logic.
  */
class SubmissionsRepository(
  dataSource: SlickDataSource,
  trackDetailedSubmissionMetrics: Boolean = true,
  override val workbenchMetricBaseName: String
) extends RawlsInstrumented {

  import dataSource.dataAccess.driver.api._


  def getActiveWorkflowsAndSetStatusToAborted(workspace: Workspace)(implicit ex: ExecutionContext): Future[Seq[WorkflowRecord]] =
    dataSource
      .inTransaction { dataAccess =>
        for {
          // Gather any active workflows with external ids
          workflowsToAbort <- dataAccess.workflowQuery.findActiveWorkflowsWithExternalIds(workspace)

          // If a workflow is not done, automatically change its status to Aborted
          _ <- dataAccess.workflowQuery.findWorkflowsByWorkspace(workspace).result.map { workflowRecords =>
            workflowRecords
              .filter(workflowRecord => !WorkflowStatuses.withName(workflowRecord.status).isDone)
              .foreach { workflowRecord =>
                dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Aborted) { status =>
                  if (trackDetailedSubmissionMetrics)
                    Option(
                      workflowStatusCounter(
                        workspaceSubmissionMetricBuilder(workspace.toWorkspaceName, workflowRecord.submissionId)
                      )(
                        status
                      )
                    )
                  else None
                }
              }
          }
        } yield workflowsToAbort
      }
}
