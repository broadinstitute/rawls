package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.workspace.api.{ControlledAzureResourceApi, WorkspaceApplicationApi}
import bio.terra.workspace.client.ApiClient

/**
 * Represents a way to get various workspace manager clients
 */
trait WorkspaceManagerApiClientProvider {
  def getApiClient(accessToken: String): ApiClient

  def getControlledAzureResourceApi(accessToken: String): ControlledAzureResourceApi

  def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi
}

class HttpWorkspaceManagerClientProvider(baseWorkspaceManagerUrl: String) extends WorkspaceManagerApiClientProvider {
  def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setAccessToken(accessToken)

    client
  }

  def getControlledAzureResourceApi(accessToken: String): ControlledAzureResourceApi = {
    new ControlledAzureResourceApi(getApiClient(accessToken))
  }

  def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi = {
    new WorkspaceApplicationApi(getApiClient(accessToken))
  }
}
