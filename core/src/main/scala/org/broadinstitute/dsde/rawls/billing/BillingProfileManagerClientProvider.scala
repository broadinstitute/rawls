package org.broadinstitute.dsde.rawls.billing

import bio.terra.common.tracing.JakartaTracingFilter
import bio.terra.profile.api.{AzureApi, ProfileApi, SpendReportingApi, UnauthenticatedApi}
import bio.terra.profile.client.ApiClient
import io.opencensus.trace.Tracing
import io.opentelemetry.api.GlobalOpenTelemetry
import jakarta.ws.rs.client.ClientBuilder
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.{TracingUtils, WithOtelContextFilter}
import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.jnh.connector.JavaNetHttpConnectorProvider

/**
 * Implementors of this trait know how to instantiate billing profile manager client
 * classes for use with the billing profile manager service.
 */
trait BillingProfileManagerClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient

  def getAzureApi(ctx: RawlsRequestContext): AzureApi

  def getProfileApi(ctx: RawlsRequestContext): ProfileApi

  def getSpendReportingApi(ctx: RawlsRequestContext): SpendReportingApi

  def getUnauthenticatedApi(): UnauthenticatedApi
}

class HttpBillingProfileManagerClientProvider(baseBpmUrl: Option[String]) extends BillingProfileManagerClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient = {
    val client: ApiClient = new ApiClient()

    // By default, the client uses the `HttpUrlConnectorProvider` which relies on a workaround for
    // PATCH endpoints that is incompatible with Java 17. Specifying a different ConnectorProvider
    // allows us to call PATCH endpoints in BPM.
    val clientConfig = new ClientConfig()
    clientConfig.connectorProvider(new JavaNetHttpConnectorProvider())
    client.setHttpClient(ClientBuilder.newClient(clientConfig))

    TracingUtils.enableCrossServiceTracing(client.getHttpClient, ctx)
    client.setBasePath(basePath)
    client.setAccessToken(ctx.userInfo.accessToken.token)
    client
  }

  override def getAzureApi(ctx: RawlsRequestContext): AzureApi =
    new AzureApi(getApiClient(ctx))

  override def getProfileApi(ctx: RawlsRequestContext): ProfileApi =
    new ProfileApi(getApiClient(ctx))

  override def getSpendReportingApi(ctx: RawlsRequestContext): SpendReportingApi =
    new SpendReportingApi(getApiClient(ctx))

  private def basePath =
    baseBpmUrl.getOrElse(throw new NotImplementedError("Billing profile manager path not configured"))

  override def getUnauthenticatedApi(): UnauthenticatedApi = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(basePath)
    new UnauthenticatedApi(client)
  }
}
