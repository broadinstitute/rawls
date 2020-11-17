package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.rbs.generated.ApiClient
import bio.terra.rbs.generated.controller.RbsApi
import bio.terra.rbs.generated.model.{PoolInfo, ResourceInfo}
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.ExecutionContext

class HttpResourceBufferDAO(config: ResourceBufferConfig, clientServiceAccountCreds: Credential)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends ResourceBufferDAO {

  private val baseUrl = config.url

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseUrl)
    client.setAccessToken(accessToken)

    client
  }

  private def getResourceBufferApi(accessToken: OAuth2BearerToken) = {
    new RbsApi(getApiClient(accessToken.token))
  }

  private def handoutResourceGeneric(poolId: PoolId, handoutRequestId: String, accessToken: OAuth2BearerToken): ResourceInfo =
    getResourceBufferApi(accessToken).handoutResource(poolId.value, handoutRequestId)

  override def getPoolInfo(poolId: PoolId): PoolInfo = {
    getResourceBufferApi(OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)).getPoolInfo(poolId.value)
  }

  override def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): GoogleProjectId = {
    val resource = handoutResourceGeneric(poolId, handoutRequestId, OAuth2BearerToken(clientServiceAccountCreds.getAccessToken))
    GoogleProjectId(resource.getCloudResourceUid.getGoogleProjectUid.getProjectId)
  }

  // get the corresponding projectPoolId from the config, based on the projectPoolType
  override def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType) = {
    val projectPoolId: ProjectPoolId = projectPoolType match {
      case ProjectPoolType.Regular => config.regularProjectPoolId
      case ProjectPoolType.ServicePerimeter => config.servicePerimeterProjectPoolId
    }
    projectPoolId
  }


  // todo: put in firecloud-develop rawls config
  /*

  gcs {
  ...
  pathToResourceBufferPem =
  }

  resourceBuffer {
    projectPool {
      regular = "regularProjectPoolId-manualentry"
      servicePerimeter = "servicePerimeterProjectPoolId-manualentry"
    }
    url =
  }
   */

  //todo: unit tests

}

