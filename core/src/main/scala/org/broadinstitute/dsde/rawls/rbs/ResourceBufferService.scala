package org.broadinstitute.dsde.rawls.rbs

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.rbs.generated.model.PoolInfo
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, UserInfo}

import scala.concurrent.ExecutionContext

object ResourceBufferService {
  def constructor(resourceBufferDAO: ResourceBufferDAO, serviceAccountCreds: Credential)(userInfo: UserInfo)(implicit executionContext: ExecutionContext): ResourceBufferService = {
    new ResourceBufferService(resourceBufferDAO, userInfo, serviceAccountCreds)
  }
}
// todo: delete this file? might not need this if we're just using RBS under the hood.
class ResourceBufferService(resourceBufferDAO: ResourceBufferDAO, protected val userInfo: UserInfo, serviceAccountCreds: Credential) {

  def GetPoolInfo(poolId: String): PoolInfo = getPoolInfo(poolId)
  def HandoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = handoutGoogleProject(poolId, handoutRequestId)

  def getPoolInfo(poolId: String): PoolInfo = {
    val accessToken: OAuth2BearerToken = userInfo.accessToken // todo: user access token or SA access token?
    resourceBufferDAO.getPoolInfo(poolId, accessToken)
  }

  def handoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = {
    val accessToken: OAuth2BearerToken = userInfo.accessToken // todo: user access token or SA access token?
    resourceBufferDAO.handoutGoogleProject(poolId, handoutRequestId, accessToken)
  }

}
