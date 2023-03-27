import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

class LeonardoClient(leonardoBasePath: String) {
  var leonardoBasePath: String = leonardoBasePath
  private def appV2leonardoApi(accessToken: String): AppsV2Api = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new AppsV2Api(apiClient)
  }

  def createWDSInstance(token: String, workspaceId: String, appName: String): Unit  = {
    val createAppRequest = new CreateAppRequest();
    createAppRequest.setAppType(AppType.CROMWELL);
    appV2leonardoApi(token).createAppV2(workspaceId, appName, createAppRequest);
  }

}
