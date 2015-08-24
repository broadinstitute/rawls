package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem

/**
 * Created by mbemis on 8/24/15.
 */
trait AbstractContainerDAO {

  implicit val system = ActorSystem("rawls")

  val workflowDAO: GraphWorkflowDAO
  val workspaceDAO: GraphWorkspaceDAO
  val entityDAO: GraphEntityDAO
  val methodConfigDAO: GraphMethodConfigurationDAO
  val methodRepoDAO: HttpMethodRepoDAO
  val executionServiceDAO: HttpExecutionServiceDAO
  val submissionDAO: GraphSubmissionDAO
  val gcsDAO: GoogleCloudStorageDAO


}
