package org.broadinstitute.dsde.rawls.resourcebuffer

import bio.terra.buffer.model.JobModel
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.{ExecutionContext, Future}

trait ResourceBufferService {
  def getGoogleProjectFromBuffer(projectPoolType: ProjectPoolType = ProjectPoolType.Regular,
                                 handoutRequestId: String
  ): Future[GoogleProjectId]
  def toProjectPoolId(projectPoolType: ProjectPoolType): ProjectPoolId

  def serviceAccountEmail: String

  def repairGoogleProject(googleProjectId: String): Future[JobModel]

  def getGoogleProjectRepairJobs(googleProjectId: String): Future[java.util.List[JobModel]]

  def getJobDetails(googleProjectId: String): Future[Object]
}
