package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import bio.terra.datarepo.model.SnapshotModel

import java.util.UUID

class HttpDataRepoDAO(dataRepoInstanceName: String, dataRepoInstanceBasePath: String) extends DataRepoDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(dataRepoInstanceBasePath)
    client.setAccessToken(accessToken)

    client
  }

  private def getRepositoryApi(accessToken: OAuth2BearerToken) = {
    new RepositoryApi(getApiClient(accessToken.token))
  }

  override def getInstanceName: String = dataRepoInstanceName

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = {
    getRepositoryApi(accessToken).retrieveSnapshot(snapshotId.toString)
  }
}
