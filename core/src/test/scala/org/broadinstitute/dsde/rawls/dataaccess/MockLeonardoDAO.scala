package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api

import java.util.UUID
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

class MockLeonardoDAO(baseUrl: String, wdsType: String) extends LeonardoDAO {

  def createApp(token: String, workspaceId: UUID, appName: String, appType: String): Unit =
    return

  def getAppsV2leonardoApi(accessToken: String): AppsV2Api =
    return new AppsV2Api(new ApiClient())

  def createWDSInstance(token: String, workspaceId: UUID, appName: String): Unit =
    return

}
