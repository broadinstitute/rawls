package org.broadinstitute.dsde.rawls.resourcebuffer

import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

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

}
