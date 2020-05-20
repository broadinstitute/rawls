package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel
import com.typesafe.config.ConfigFactory

class HttpDataRepoDAO extends DataRepoDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    // TODO: should be injected, not read directly from conf!
    client.setBasePath(ConfigFactory.load().getString("dataRepo.terraInstance"))
    client.setAccessToken(accessToken)

    client
  }

  private def getRepositoryApi(accessToken: OAuth2BearerToken) = {
    new RepositoryApi(getApiClient(accessToken.token))
  }

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = {
    getRepositoryApi(accessToken).retrieveSnapshot(snapshotId.toString)
  }
}