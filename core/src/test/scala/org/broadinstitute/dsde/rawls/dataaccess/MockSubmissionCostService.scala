package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(defaultTableName: String, serviceProject: String, billingSearchWindowDays: Int, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(defaultTableName, serviceProject, billingSearchWindowDays, bigQueryDAO) {
  val fixedCost = 1.23f
  override def getSubmissionCosts(submissionId: String, workflowIds: Seq[String], workspaceNamespace: String, submissionDate: DateTime, submissionDoneDate: Option[DateTime], tableNameOpt: Option[String] = Option(defaultTableName)): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> fixedCost).toMap)
  }

  override def getWorkflowCost(workflowId: String, workspaceNamespace: String, submissionDate: DateTime, submissionDoneDate: Option[DateTime], tableNameOpt: Option[String] = Option(defaultTableName)): Future[Map[String, Float]] = {
    Future(Map(workflowId -> fixedCost))
  }
}
