package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO

import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(tableName: String, serviceProject: String, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(tableName, serviceProject, bigQueryDAO) {
  val fixedCost = 1.23f
  override def getWorkflowCosts(workflowIds: Seq[String], workspaceNamespace: String): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> fixedCost).toMap)
  }
}
