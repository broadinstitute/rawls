package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(tableName: String, serviceProject: String, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(tableName, serviceProject, bigQueryDAO) {
  override def getWorkflowCosts(workflowIds: Seq[String], googleProject: GoogleProject): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> 0.0f).toMap)
  }
}
