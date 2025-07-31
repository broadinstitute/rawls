package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.buffer.api.{BufferApi, JobsApi, ResourceApi}
import bio.terra.buffer.client.{ApiClient, ApiException}
import bio.terra.buffer.model.{HandoutRequestBody, JobModel, ResourceInfo, SqlSortDirectionDescDefault}
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

  override def repairResource(googleProjectId: String): Future[JobModel] = {
    clientServiceAccountCreds.refreshToken()
    val accessToken = OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)
    retry(when500) { () =>
      Future {
        getResourceApi(accessToken).repairResource(googleProjectId)
      }
    }
  }

  override def enumerateJobs(offset: Integer,
                    limit: Integer,
                    direction: SqlSortDirectionDescDefault,
                    className: String,
                    inputs: java.util.List[String]): Future[java.util.List[JobModel]] = {
    clientServiceAccountCreds.refreshToken()
    val accessToken = OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)
    retry(when500) { () =>
      Future {
        getJobsApi(accessToken).enumerateJobs(offset, limit, direction, className, inputs)
      }
    }
  }

  override def getJob(jobId: String): Future[JobModel] = {
    clientServiceAccountCreds.refreshToken()
    val accessToken = OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)
    retry(when500) { () =>
      Future {
        getJobsApi(accessToken).retrieveJob(jobId)
      }
    }
  }

  override def getJobResult(jobId: String): Future[Object] = {
    clientServiceAccountCreds.refreshToken()
    val accessToken = OAuth2BearerToken(clientServiceAccountCreds.getAccessToken)
    retry(when500) { () =>
      Future {
        getJobsApi(accessToken).retrieveJobResult(jobId)
      }
    }
  }

  private def getResourceBufferApi(accessToken: OAuth2BearerToken) =
    new BufferApi(getApiClient(accessToken.token))

  private def getResourceApi(accessToken: OAuth2BearerToken) =
    new ResourceApi(getApiClient(accessToken.token))

  private def getJobsApi(accessToken: OAuth2BearerToken) =
    new JobsApi(getApiClient(accessToken.token))

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseUrl)
    client.setAccessToken(accessToken)

    client
  }

}
