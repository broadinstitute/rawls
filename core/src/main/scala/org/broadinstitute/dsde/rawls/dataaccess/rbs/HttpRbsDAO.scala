package org.broadinstitute.dsde.rawls.dataaccess.rbs

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.rbs.generated.model.{PoolInfo, ResourceInfo}
import bio.terra.rbs.generated.ApiClient
import bio.terra.rbs.generated.controller.RbsApi
import org.broadinstitute.dsde.rawls.model.GoogleProjectId

class HttpRbsDAO() extends RbsDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setAccessToken(accessToken)

    client
  }

  // todo: does this need the auth token?
  private def getRbsApi(accessToken: OAuth2BearerToken) = {
    new RbsApi(getApiClient(accessToken.token))
  }

  //todo: add this retry logic for handoutResource?
  // if 404, that means no resource in pool, try again later
  //  protected def when404or500(throwable: Throwable): Boolean = {
  //    throwable match {
  //      case t: RawlsExceptionWithErrorReport =>
  //        t.errorReport.statusCode.exists(status => (status.intValue/100 == 5) || status.intValue == 404)
  //      case _ => false
  //    }
  //  }

  private def handoutResourceGeneric(poolId: String, handoutRequestId: String, accessToken: OAuth2BearerToken): ResourceInfo =
    getRbsApi(accessToken).handoutResource(poolId, handoutRequestId)

  override def getPoolInfo(poolId: String, accessToken: OAuth2BearerToken): PoolInfo = {
    getRbsApi(accessToken).getPoolInfo(poolId)
  }

  // todo: make a PoolId type?
  override def handoutGoogleProject(poolId: String, handoutRequestId: String, accessToken: OAuth2BearerToken): GoogleProjectId = {
    val resource = handoutResourceGeneric(poolId, handoutRequestId, accessToken)
    GoogleProjectId(resource.getCloudResourceUid.getGoogleProjectUid.getProjectId)
  }

}

