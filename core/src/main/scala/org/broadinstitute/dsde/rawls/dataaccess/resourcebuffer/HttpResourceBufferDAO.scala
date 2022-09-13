package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.buffer.api.BufferApi
import bio.terra.buffer.client.{ApiClient, ApiException}
import bio.terra.buffer.model.{HandoutRequestBody, ResourceInfo}
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.{ExecutionContext, Future}

class HttpResourceBufferDAO(config: ResourceBufferConfig, clientServiceAccountCreds: Credential)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends ResourceBufferDAO
    with Retry
    with LazyLogging {

  private val baseUrl = config.url

  override def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId] =
    retry(when500) { () =>
      Future {
        clientServiceAccountCreds.refreshToken()
        val resource = handoutResourceGeneric(projectPoolId.value,
                                              handoutRequestId,
                                              OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)
        )
        GoogleProjectId(resource.getCloudResourceUid.getGoogleProjectUid.getProjectId)
      }
    }

  protected def when500(throwable: Throwable): Boolean =
    throwable match {
      case t: ApiException => t.getCode / 100 == 5
      case _               => false
    }

  private def handoutResourceGeneric(poolId: String,
                                     handoutRequestId: String,
                                     accessToken: OAuth2BearerToken
  ): ResourceInfo =
    getResourceBufferApi(accessToken).handoutResource(new HandoutRequestBody().handoutRequestId(handoutRequestId),
                                                      poolId
    )

  private def getResourceBufferApi(accessToken: OAuth2BearerToken) =
    new BufferApi(getApiClient(accessToken.token))

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseUrl)
    client.setAccessToken(accessToken)

    client
  }

}
