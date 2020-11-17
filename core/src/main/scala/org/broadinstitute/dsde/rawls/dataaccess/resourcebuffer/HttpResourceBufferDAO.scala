package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.rbs.generated.ApiClient
import bio.terra.rbs.generated.controller.RbsApi
import bio.terra.rbs.generated.model.ResourceInfo
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId, ProjectPoolId, ProjectPoolType}
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.{ExecutionContext, Future}

class HttpResourceBufferDAO(config: ResourceBufferConfig, clientServiceAccountCreds: Credential)
                           (implicit val system: ActorSystem, val materializer: Materializer,
                            val executionContext: ExecutionContext) extends ResourceBufferDAO with Retry with LazyLogging {

  private val baseUrl = config.url

  override def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): Future[GoogleProjectId] = {
    retry(when500) { () =>
      Future {
        val resource = handoutResourceGeneric(poolId, handoutRequestId, OAuth2BearerToken(clientServiceAccountCreds.getAccessToken))
        GoogleProjectId(resource.getCloudResourceUid.getGoogleProjectUid.getProjectId)
      }
    }
  }

  protected def when500(throwable: Throwable): Boolean = {
    throwable match {
      case t: RawlsExceptionWithErrorReport => t.errorReport.statusCode.exists(_.intValue / 100 == 5)
      case _ => false
    }
  }

  private def handoutResourceGeneric(poolId: PoolId, handoutRequestId: String, accessToken: OAuth2BearerToken): ResourceInfo =
    getResourceBufferApi(accessToken).handoutResource(poolId.value, handoutRequestId)

  private def getResourceBufferApi(accessToken: OAuth2BearerToken) = {
    new RbsApi(getApiClient(accessToken.token))
  }

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseUrl)
    client.setAccessToken(accessToken)

    client
  }

  override def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType): ProjectPoolId = {
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

