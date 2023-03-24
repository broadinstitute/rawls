import org.broadinstitute.dsde.workbench.client.leonardo.api.RuntimesApi
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.model.GetRuntimeResponse

class LeonardoClient(leonardoBasePath: String) {
  private def leonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new RuntimesApi(apiClient)
  }

  def createWDSInstance(token: String, workspaceId: String): GetRuntimeResponse = {
    val leonardoApi = leonardoApi(token)
    leonardoApi.getAzureRuntime(workspaceId, runtimeName)
  }
}