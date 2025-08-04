package org.broadinstitute.dsde.rawls.resourcebuffer

import bio.terra.buffer.model.{JobModel, SqlSortDirectionDescDefault}
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.{ExecutionContext, Future}

object ResourceBufferServiceImpl {
  def constructor(resourceBufferDAO: ResourceBufferDAO, config: ResourceBufferConfig)(implicit
    executionContext: ExecutionContext
  ): ResourceBufferServiceImpl =
    new ResourceBufferServiceImpl(resourceBufferDAO, config)
}
class ResourceBufferServiceImpl(resourceBufferDAO: ResourceBufferDAO, config: ResourceBufferConfig)
    extends ResourceBufferService {

  def getGoogleProjectFromBuffer(projectPoolType: ProjectPoolType = ProjectPoolType.Regular,
                                 handoutRequestId: String
  ): Future[GoogleProjectId] = {
    val projectPoolId: ProjectPoolId = toProjectPoolId(projectPoolType)
    resourceBufferDAO.handoutGoogleProject(projectPoolId, handoutRequestId)
  }

  def toProjectPoolId(projectPoolType: ProjectPoolType): ProjectPoolId = {
    val projectPoolId: ProjectPoolId = projectPoolType match {
      case ProjectPoolType.Regular                => config.regularProjectPoolId
      case ProjectPoolType.ExfiltrationControlled => config.exfiltrationControlledPoolId
    }
    projectPoolId
  }

  def serviceAccountEmail: String =
    config.saEmail

  def repairGoogleProject(googleProjectId: String): Future[JobModel] =
    resourceBufferDAO.repairResource(googleProjectId)

  def getRepairGoogleProjectStatus(googleProjectId: String): Future[JobModel] =
    getRepairGoogleProjectJob(googleProjectId).flatMap {
      case Some(job) => resourceBufferDAO.getJob(job.getId)
      case None      => Future.failed(new Exception(s"No repair job found for project $googleProjectId"))
    }

  def getRepairGoogleProjectJob(googleProjectId: String): Future[Option[JobModel]] =
    resourceBufferDAO
      .enumerateJobs(
        0,
        10,
        SqlSortDirectionDescDefault.DESC,
        "bio.terra.buffer.service.resource.flight.GoogleProjectRepairFlight",
        java.util.List.of("googleProjectId=" + googleProjectId)
      )
      .map(_.headOption)

  def getJobDetails(jobId: String): Future[Object] =
    resourceBufferDAO.getJobResult(jobId)

}
