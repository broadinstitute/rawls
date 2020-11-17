package org.broadinstitute.dsde.rawls.resourcebuffer

import java.util.UUID

import bio.terra.rbs.generated.model.PoolInfo
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

object ResourceBufferService {
  def constructor(resourceBufferDAO: ResourceBufferDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext): ResourceBufferService = {
    new ResourceBufferService(resourceBufferDAO, userInfo)
  }
}
class ResourceBufferService(resourceBufferDAO: ResourceBufferDAO, protected val userInfo: UserInfo) {

  def GetPoolInfo(poolId: String): PoolInfo = getPoolInfo(poolId)
  def GetGoogleProjectFromRBS(projectPoolType: ProjectPoolType): Future[GoogleProjectId] = getGoogleProjectFromRBS(projectPoolType)

  def getPoolInfo(poolId: String): PoolInfo = {
    resourceBufferDAO.getPoolInfo(poolId)
  }

  def getGoogleProjectFromRBS(projectPoolType: ProjectPoolType = ProjectPoolType.Regular): Future[GoogleProjectId] = {

    val projectPoolId: ProjectPoolId = resourceBufferDAO.getProjectPoolId(projectPoolType)

    // todo: doesn't seem like this is too important to save since it's only for getting back the same info we already got. verify this?
    val handoutRequestId = generateHandoutRequestId(userInfo, projectPoolId)

    val project = resourceBufferDAO.handoutGoogleProject(projectPoolId.value, handoutRequestId)
    Future.successful(project)
  }

  //  handoutRequestId:
  //        The unique identifier presented by the client for a resource request.
  //        Using the same handoutRequestId in the same pool would get the same resource back.
  private def generateHandoutRequestId(userInfo: UserInfo, projectPoolId: ProjectPoolId): String = {
    val prefix: String = userInfo.userSubjectId + projectPoolId.value
    prefix + UUID.randomUUID().toString
  }

}
