package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class MockSubmissionCostService(bigQueryDAO: MockHttpGoogleBigQueryDAO)(implicit executionContext: ExecutionContext) extends SubmissionCostService(bigQueryDAO) {
  override def getWorkflowCosts(namespace: String, workflowIds: Seq[String], userInfo: UserInfo, googleProject: GoogleProject): Future[Map[String, Float]] = {
    Future(workflowIds.map(_ -> 0.00f).toMap)
  }
}
