package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(tableName: String, serviceProject: String, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(tableName, serviceProject, bigQueryDAO) {
  val fixedCost = 1.23f
  override def getSubmissionCosts(submissionId: String, workflowIds: Seq[String], workspaceNamespace: String, submissionDate: DateTime, endDate: DateTime): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> fixedCost).toMap)
  }

  override def getWorkflowCost(workflowId: String, workspaceNamespace: String, submissionDate: DateTime, endDate: DateTime): Future[Map[String, Float]] = {
    Future(Map(workflowId -> fixedCost))
  }
}
