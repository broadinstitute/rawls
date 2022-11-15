package org.broadinstitute.dsde.rawls.billing

import bio.terra.common.tracing.JerseyTracingFilter
import bio.terra.profile.api.{AzureApi, ProfileApi, UnauthenticatedApi}
import bio.terra.profile.client.ApiClient
import io.opencensus.trace.Tracing
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.WithSpanFilter
import org.glassfish.jersey.client.ClientConfig

/**
 * Implementors of this trait know how to instantiate billing profile manager client
 * classes for use with the billing profile manager service.
 */
trait BillingProfileManagerClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient

  def getAzureApi(ctx: RawlsRequestContext): AzureApi

  def getProfileApi(ctx: RawlsRequestContext): ProfileApi

  def getUnauthenticatedApi(): UnauthenticatedApi
}

class HttpBillingProfileManagerClientProvider(baseBpmUrl: Option[String]) extends BillingProfileManagerClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient = {
    val client: ApiClient = new ApiClient() {
      override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
        super.performAdditionalClientConfiguration(clientConfig)
        ctx.tracingSpan.foreach { span =>
          clientConfig.register(new WithSpanFilter(span))
          clientConfig.register(new JerseyTracingFilter(Tracing.getTracer))
        }
      }
    }
    client.setBasePath(basePath)
    client.setAccessToken(ctx.userInfo.accessToken.token)

    client
  }

  override def getAzureApi(ctx: RawlsRequestContext): AzureApi =
    new AzureApi(getApiClient(ctx))

  override def getProfileApi(ctx: RawlsRequestContext): ProfileApi =
    new ProfileApi(getApiClient(ctx))

  private def basePath =
    baseBpmUrl.getOrElse(throw new NotImplementedError("Billing profile manager path not configured"))

  override def getUnauthenticatedApi(): UnauthenticatedApi =
    new UnauthenticatedApi()
}
