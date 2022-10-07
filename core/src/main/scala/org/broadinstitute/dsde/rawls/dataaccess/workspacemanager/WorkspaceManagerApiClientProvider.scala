package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.common.tracing.JerseyTracingFilter
import bio.terra.workspace.api.{ControlledAzureResourceApi, WorkspaceApi, WorkspaceApplicationApi}
import bio.terra.workspace.client.ApiClient
import io.opencensus.common.Scope
import io.opencensus.trace.{Span, Tracing}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.WithSpanFilter
import org.glassfish.jersey.client.ClientConfig

import javax.ws.rs.client.{ClientRequestContext, ClientRequestFilter, ClientResponseContext, ClientResponseFilter}
import javax.ws.rs.ext.Provider

/**
 * Represents a way to get various workspace manager clients
 */
trait WorkspaceManagerApiClientProvider {
  def getApiClient(ctx: RawlsRequestContext): ApiClient

  def getControlledAzureResourceApi(ctx: RawlsRequestContext): ControlledAzureResourceApi

  def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi

  def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi
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

  def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi =
    new WorkspaceApplicationApi(getApiClient(ctx))

  def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
    new WorkspaceApi(getApiClient(ctx))
}
