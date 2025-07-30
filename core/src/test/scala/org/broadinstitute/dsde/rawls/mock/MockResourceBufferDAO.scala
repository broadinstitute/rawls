package org.broadinstitute.dsde.rawls.mock

import bio.terra.buffer.model.JobModel
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}

import scala.concurrent.Future

class MockResourceBufferDAO extends ResourceBufferDAO {

  def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId] = {
    val googleProjectId = GoogleProjectId("project-from-buffer")
    Future.successful(googleProjectId)
  }

  override def repairResource(googleProjectId: String): Future[JobModel] = {
    val jobModel = new JobModel()
    jobModel.setId("test-job-id")
    jobModel.setJobStatus(JobModel.JobStatusEnum.RUNNING)
    jobModel.setDescription("test-description")
    jobModel.setClassName("test-class-name")
    Future.successful(jobModel)
  }
}
