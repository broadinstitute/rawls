package org.broadinstitute.dsde.rawls.mock

import bio.terra.buffer.model.{JobModel, SqlSortDirectionDescDefault}
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

  override def enumerateJobs(offset: Integer,
                    limit: Integer,
                    direction: SqlSortDirectionDescDefault,
                    className: String,
                    inputs: java.util.List[String]
                   ): Future[java.util.List[JobModel]] =
    {
      val jobModel = new JobModel()
      jobModel.setId("test-job-id")
      jobModel.setJobStatus(JobModel.JobStatusEnum.RUNNING)
      jobModel.setDescription("test-description")
      jobModel.setClassName("test-class-name")

      val jobModel2 = new JobModel()
      jobModel.setId("test-job-id-2")
      jobModel.setJobStatus(JobModel.JobStatusEnum.RUNNING)
      jobModel.setDescription("test-description")
      jobModel.setClassName("test-other-class-name")
      Future.successful(java.util.List.of(jobModel, jobModel2))
    }

  override def getJob(jobId: String): Future[JobModel] = {
    val jobModel = new JobModel()
    jobModel.setId("test-job-id")
    jobModel.setJobStatus(JobModel.JobStatusEnum.RUNNING)
    jobModel.setDescription("test-description")
    jobModel.setClassName("test-class-name")
    Future.successful(jobModel)
  }

  override def getJobResult(jobId: String): Future[Object] = Future.successful(new Object())
}
