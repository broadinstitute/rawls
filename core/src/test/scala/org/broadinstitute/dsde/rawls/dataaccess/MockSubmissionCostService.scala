package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(tableName: String, bigQueryDAO: MockHttpGoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(tableName, bigQueryDAO) {
  override def getWorkflowCosts(namespace: String, workflowIds: Seq[String], googleProject: GoogleProject): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> 0.00f).toMap)
  }
}
