package org.broadinstitute.dsde.rawls.dataaccess.policyservice

import bio.terra.policy.api.TpsApi
import bio.terra.policy.client.ApiClient
import bio.terra.policy.model.{
  TpsComponent,
  TpsObjectType,
  TpsPaoCreateRequest,
  TpsPolicyInput,
  TpsPolicyInputs,
  TpsPolicyPair
}
import jakarta.ws.rs.client.ClientBuilder
import org.broadinstitute.dsde.rawls.model.{ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.util.TracingUtils
import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.jnh.connector.JavaNetHttpConnectorProvider

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class HttpPolicyServiceDAO(policyServiceUrl: String)(implicit val ec: ExecutionContext) extends PolicyServiceDAO {
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
    client.setBasePath(policyServiceUrl)
    client.setAccessToken(ctx.userInfo.accessToken.token)
    client
  }

  protected def getTpsApi(ctx: RawlsRequestContext): TpsApi =
    new TpsApi(getApiClient(ctx))

  def createWorkspacePao(workspaceId: UUID,
                         workspaceRequest: WorkspaceRequest,
                         ctx: RawlsRequestContext
  ): Future[Unit] = Future {
    blocking {
      val req =
        new TpsPaoCreateRequest()
          .objectType(TpsObjectType.WORKSPACE)
          .objectId(workspaceId)
          .component(TpsComponent.RAWLS)
      val protectedDataPolicy = new TpsPolicyInput().namespace("terra").name("protected-data")

      (workspaceRequest.authorizationDomain, workspaceRequest.enhancedBucketLogging) match {
        case (Some(authDomain), _) if authDomain.nonEmpty =>
          val authDomainGroups = authDomain.map { case ManagedGroupRef(RawlsGroupName(membersGroupName)) =>
            new TpsPolicyPair().key("group").value(membersGroupName)
          }
          val groupConstraintPolicy = new TpsPolicyInput()
            .namespace("terra")
            .name("group-constraint")
            .additionalData(authDomainGroups.toList.asJava)
          req.setAttributes(new TpsPolicyInputs().inputs(List(protectedDataPolicy, groupConstraintPolicy).asJava))
        case (None, Some(enhancedBucketLogging)) if enhancedBucketLogging =>
          req.setAttributes(new TpsPolicyInputs().inputs(List(protectedDataPolicy).asJava))
        case _ =>
      }

      getTpsApi(ctx).createPao(req)
    }
  }
}
