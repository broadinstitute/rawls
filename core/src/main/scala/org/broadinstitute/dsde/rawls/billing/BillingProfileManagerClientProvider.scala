package org.broadinstitute.dsde.rawls.billing

import bio.terra.profile.api.{AzureApi, ProfileApi}
import bio.terra.profile.client.ApiClient

/**
 * Implementors of this trait know how to instantiate billing profile manager client
 * classes for use with the billing profile manager service.
 */
trait BillingProfileManagerClientProvider {
  def getApiClient(accessToken: String): ApiClient

  def getAzureApi(accessToken: String): AzureApi

  def getProfileApi(accessToken: String): ProfileApi
}

class HttpBillingProfileManagerClientProvider(baseBpmUrl: Option[String]) extends BillingProfileManagerClientProvider {
  override def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(basePath)
    client.setAccessToken(accessToken)

    client
  }

  override def getAzureApi(accessToken: String): AzureApi =
    new AzureApi(getApiClient(accessToken))

  override def getProfileApi(accessToken: String): ProfileApi =
    new ProfileApi(getApiClient(accessToken))

  private def basePath =
    baseBpmUrl.getOrElse(throw new NotImplementedError("Billing profile manager path not configured"))
}
