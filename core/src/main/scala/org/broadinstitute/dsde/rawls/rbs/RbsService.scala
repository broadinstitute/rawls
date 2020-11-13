package org.broadinstitute.dsde.rawls.rbs

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.rbs.generated.model.PoolInfo
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.dataaccess.rbs.RbsDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, UserInfo}

import scala.concurrent.ExecutionContext

object RbsService {
  def constructor(rbsDao: RbsDAO, serviceAccountCreds: Credential)(userInfo: UserInfo)(implicit executionContext: ExecutionContext): RbsService = {
    new RbsService(rbsDao, userInfo, serviceAccountCreds)
  }
}
// todo: delete this file? might not need this if we're just using RBS under the hood.
class RbsService(rbsDao: RbsDAO, protected val userInfo: UserInfo, serviceAccountCreds: Credential) {

  def GetPoolInfo(poolId: String): PoolInfo = getPoolInfo(poolId)
  def HandoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = handoutGoogleProject(poolId, handoutRequestId)

  def getPoolInfo(poolId: String): PoolInfo = {
    val accessToken: OAuth2BearerToken = userInfo.accessToken // todo: user access token or SA access token?
    rbsDao.getPoolInfo(poolId, accessToken)
  }

  def handoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = {
    val accessToken: OAuth2BearerToken = userInfo.accessToken // todo: user access token or SA access token?
    rbsDao.handoutGoogleProject(poolId, handoutRequestId, accessToken)
  }

}
