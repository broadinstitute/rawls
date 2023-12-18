package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.common.tracing.JerseyTracingFilter
import bio.terra.workspace.api.{ControlledAzureResourceApi, ControlledGcpResourceApi, JobsApi, LandingZonesApi, ResourceApi, UnauthenticatedApi, WorkspaceApi, WorkspaceApplicationApi}
import bio.terra.workspace.client.ApiClient
import io.opencensus.trace.Tracing
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.WithSpanFilter
import org.glassfish.jersey.client.ClientConfig

/**
 * Represents a way to get various workspace manager clients
 */
trait WorkspaceManagerApiClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient

  def getJobsApi(ctx: RawlsRequestContext): JobsApi

  def getControlledAzureResourceApi(ctx: RawlsRequestContext): ControlledAzureResourceApi

  def getControlledGcpResourceApi(ctx: RawlsRequestContext): ControlledGcpResourceApi

  def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi

  def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi

  def getLandingZonesApi(ctx: RawlsRequestContext): LandingZonesApi

  def getResourceApi(ctx: RawlsRequestContext): ResourceApi

  def getUnauthenticatedApi(): UnauthenticatedApi

}

class HttpWorkspaceManagerClientProvider(baseWorkspaceManagerUrl: String) extends WorkspaceManagerApiClientProvider {
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
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setAccessToken(ctx.userInfo.accessToken.token)

    client
  }

  def getControlledAzureResourceApi(ctx: RawlsRequestContext): ControlledAzureResourceApi =
    new ControlledAzureResourceApi(getApiClient(ctx))

  def getControlledGcpResourceApi(ctx: RawlsRequestContext): ControlledGcpResourceApi =
    new ControlledGcpResourceApi(getApiClient(ctx))

  def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi =
    new WorkspaceApplicationApi(getApiClient(ctx))

  def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
    new WorkspaceApi(getApiClient(ctx))

  def getLandingZonesApi(ctx: RawlsRequestContext): LandingZonesApi =
    new LandingZonesApi(getApiClient(ctx))

  def getJobsApi(ctx: RawlsRequestContext): JobsApi = new JobsApi(getApiClient(ctx))

  def getResourceApi(ctx: RawlsRequestContext): ResourceApi =
    new ResourceApi(getApiClient(ctx))

  override def getUnauthenticatedApi(): UnauthenticatedApi = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl)
    new UnauthenticatedApi(client)
  }
}
