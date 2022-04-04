package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.workspace.api.WorkspaceApplicationApi
import bio.terra.workspace.client.ApiClient

/**
 * Represents a way to get various workspace manager clients
 */
trait WorkspaceManagerApiClientProvider {
  def getApiClient(accessToken: String): ApiClient

  def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi
}

class HttpWorkspaceManagerClientProvider(baseWorkspaceManagerUrl: String) extends WorkspaceManagerApiClientProvider {
  def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setAccessToken(accessToken)

    client
  }

  def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi = {
    new WorkspaceApplicationApi(getApiClient(accessToken))
  }
}
