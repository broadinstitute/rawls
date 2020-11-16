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
  def HandoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = handoutGoogleProject(poolId, handoutRequestId)

  private def getPoolInfo(poolId: String): PoolInfo = {
    resourceBufferDAO.getPoolInfo(poolId)
  }

  private def handoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = {
    resourceBufferDAO.handoutGoogleProject(poolId, handoutRequestId)
  }

  def getGoogleProjectFromRBS(projectPoolType: ProjectPoolType = ProjectPoolType.Regular): Future[GoogleProjectId] = {

    val projectPoolId: ProjectPoolId = resourceBufferDAO.getProjectPoolId(projectPoolType)

    // todo: doesn't seem like this is too important to save since it's only for getting back the same info we already got. verify this?
    val handoutRequestId = generateHandoutRequestId(userInfo, projectPoolId)

    val projectFromRbs = resourceBufferDAO.handoutGoogleProject(projectPoolId.value, handoutRequestId)
    Future.successful(projectFromRbs)
  }


  //  handoutRequestId:
  //        The unique identifier presented by the client for a resource request.
  //        Using the same handoutRequestId in the same pool would ge the same resource back.
  // prefix with user and pooltype
  private def generateHandoutRequestId(userInfo: UserInfo, projectPoolId: ProjectPoolId): String = {
    val prefix: String = userInfo.userSubjectId + projectPoolId.value
    prefix + UUID.randomUUID().toString
  }

}
