package org.broadinstitute.dsde.rawls.dataaccess.tps

import bio.terra.policy.api.TpsApi
import bio.terra.policy.client.ApiClient
import bio.terra.policy.model.{TpsPaoCreateRequest, TpsPaoGetResult, TpsPaoSourceRequest, TpsPaoUpdateResult}
import com.typesafe.scalalogging.LazyLogging
import jakarta.ws.rs.client.ClientBuilder
import org.broadinstitute.dsde.rawls.credentials.RawlsCredential
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.TracingUtils
import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.jnh.connector.JavaNetHttpConnectorProvider

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class HttpTpsDAO(tpsUrl: String, rawlsSaCreds: RawlsCredential)(implicit val ec: ExecutionContext) extends TpsDAO {
  protected def getApiClient(ctx: RawlsRequestContext): ApiClient = {
    val client: ApiClient = new ApiClient()

    // By default, the client uses the `HttpUrlConnectorProvider` which relies on a workaround for
    // PATCH endpoints that is incompatible with Java 17. Specifying a different ConnectorProvider
    // allows us to call PATCH endpoints in TPS.
    val clientConfig = new ClientConfig()
    clientConfig.connectorProvider(new JavaNetHttpConnectorProvider())
    clientConfig.register(client.getJSON)
    client.setHttpClient(ClientBuilder.newClient(clientConfig))

    TracingUtils.enableCrossServiceTracing(client.getHttpClient, ctx)
    client.setBasePath(tpsUrl)
    if (rawlsSaCreds.getExpiresAt.isBefore(Instant.now.plus(5, ChronoUnit.MINUTES))) {
      rawlsSaCreds.refreshToken()
    }

    client.setAccessToken(rawlsSaCreds.getAccessToken)
    client
  }

  protected def getTpsApi(ctx: RawlsRequestContext): TpsApi =
    new TpsApi(getApiClient(ctx))

  def createPao(request: TpsPaoCreateRequest, ctx: RawlsRequestContext): Future[Unit] = Future {
    blocking {
      getTpsApi(ctx).createPao(request)
    }
  }

  def mergePao(request: TpsPaoSourceRequest, objectId: UUID, ctx: RawlsRequestContext): Future[Unit] = Future {
    blocking {
      getTpsApi(ctx).mergePao(request, objectId)
    }
  }

  def getPao(objectId: UUID, ctx: RawlsRequestContext): Future[TpsPaoGetResult] = Future {
    blocking {
      getTpsApi(ctx).getPao(objectId, false)
    }
  }

  def deletePao(objectId: UUID, ctx: RawlsRequestContext): Future[Unit] = Future {
    blocking {
      getTpsApi(ctx).deletePao(objectId)
    }
  }

  def linkPao(request: TpsPaoSourceRequest, objectId: UUID, ctx: RawlsRequestContext): Future[TpsPaoUpdateResult] =
    Future {
      blocking {
        getTpsApi(ctx).linkPao(request, objectId)
      }
    }

  def listPaos(objectIds: Seq[UUID], ctx: RawlsRequestContext): Future[Seq[TpsPaoGetResult]] = Future {
    blocking {
      getTpsApi(ctx).listPaos(objectIds.asJava).asScala.toSeq
    }
  }
}
