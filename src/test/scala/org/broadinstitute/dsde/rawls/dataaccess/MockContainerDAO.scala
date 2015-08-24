package org.broadinstitute.dsde.rawls.dataaccess

/**
 * Created by mbemis on 8/19/15.
 */

class MockContainerDAO(methodRepoServer: String, executionServiceServer: String) extends ContainerDAO(methodRepoServer, executionServiceServer) {

  //force travis re-run

  override val workspaceDAO = new GraphWorkspaceDAO
  override val entityDAO = new GraphEntityDAO
  override val methodConfigDAO = new GraphMethodConfigurationDAO
  override val methodRepoDAO = new HttpMethodRepoDAO(methodRepoServer)
  override val executionServiceDAO = new HttpExecutionServiceDAO(executionServiceServer)
  override val gcsDAO = MockGoogleCloudStorageDAO
  override val submissionDAO = new GraphSubmissionDAO(new GraphWorkflowDAO)

}
