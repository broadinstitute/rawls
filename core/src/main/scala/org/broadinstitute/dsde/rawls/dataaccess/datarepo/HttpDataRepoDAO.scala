package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel

class HttpDataRepoDAO(baseDataRepoUrl: String) extends DataRepoDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseDataRepoUrl)
    client.setAccessToken(accessToken)

    client
  }

  private def getRepositoryApi(accessToken: OAuth2BearerToken) = {
    new RepositoryApi(getApiClient(accessToken.token))
  }

  override def getBaseURL: String = baseDataRepoUrl

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = {
    getRepositoryApi(accessToken).retrieveSnapshot(snapshotId.toString)
  }
}
