import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.model.GetAppResponse
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType

class LeonardoClient(leonardoBasePath: String) {
  def AppsV2leonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new AppsV2Api(apiClient)
  }

  def createWDSInstance(workspaceId: String, appName: String): T = {
    CreateAppRequest createAppRequest = new CreateAppRequest()
    // WDS supports CROMWELL appTypes for now
    createAppRequest.setAppType(AppType.CROMWELL)
    AppsV2leonardoApi appsV2leonardoApi = new AppsV2leonardoApi(token)
    appsV2leonardoApi.createAppV2(workspaceId, appName, createAppRequest)
  }
}