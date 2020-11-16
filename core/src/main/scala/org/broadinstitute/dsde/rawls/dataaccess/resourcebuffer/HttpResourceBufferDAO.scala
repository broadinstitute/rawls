package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.rbs.generated.model.{PoolInfo, ResourceInfo}
import bio.terra.rbs.generated.ApiClient
import bio.terra.rbs.generated.controller.RbsApi
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

class HttpResourceBufferDAO(config: ResourceBufferConfig, clientServiceAccountCreds: Credential) extends ResourceBufferDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setAccessToken(accessToken)

    client
  }

  // todo: does this need the auth token?
  private def getResourceBufferApi(accessToken: OAuth2BearerToken) = {
    new RbsApi(getApiClient(accessToken.token))
  }

  //todo: add this retry logic for handoutResource? checking with yonghao if we need retry logic or not
  // if 404, that means no resource in pool, try again later
  //  protected def when404or500(throwable: Throwable): Boolean = {
  //    throwable match {
  //      case t: RawlsExceptionWithErrorReport =>
  //        t.errorReport.statusCode.exists(status => (status.intValue/100 == 5) || status.intValue == 404)
  //      case _ => false
  //    }
  //  }

  private def handoutResourceGeneric(poolId: String, handoutRequestId: String, accessToken: OAuth2BearerToken): ResourceInfo =
    getResourceBufferApi(accessToken).handoutResource(poolId, handoutRequestId)

  override def getPoolInfo(poolId: String): PoolInfo = {
    getResourceBufferApi(OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)).getPoolInfo(poolId)
  }

  override def handoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId = {
    val resource = handoutResourceGeneric(poolId, handoutRequestId, OAuth2BearerToken(clientServiceAccountCreds.getAccessToken))
    GoogleProjectId(resource.getCloudResourceUid.getGoogleProjectUid.getProjectId)
  }

  // get the corresponding projectPoolId from the config, based on the projectPoolType
  def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType) = {
    val projectPoolId: ProjectPoolId = projectPoolType match {
      case ProjectPoolType.Regular => ProjectPoolId(config.regularProjectPoolId)
      case ProjectPoolType.ServicePerimeter => ProjectPoolId(config.servicePerimeterProjectPoolId)
    }
    projectPoolId
  }

  // todo: put in firecloud-develop rawls config
  /*
  resourceBuffer {
    projectPool {
      regular = "regularProjectPoolId-manualentry"
      servicePerimeter = "servicePerimeterProjectPoolId-manualentry"
    }
  }
   */

  //todo: unit tests

}

