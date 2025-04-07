package org.broadinstitute.dsde.rawls.dataaccess.policyservice

import bio.terra.policy.api.TpsApi
import bio.terra.policy.client.ApiClient
import jakarta.ws.rs.client.ClientBuilder
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.TracingUtils
import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.jnh.connector.JavaNetHttpConnectorProvider

class HttpPolicyServiceDAO(policyServiceUrl: String) extends PolicyServiceDAO {
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

  protected def getTpsApi(ctx: RawlsRequestContext): TpsApi = {
    new TpsApi(getApiClient(ctx))
  }
}
