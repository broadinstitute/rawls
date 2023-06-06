package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import bio.terra.datarepo.model.{ColumnModel, SnapshotModel, TableDataType}
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoBigQuerySupport

import java.util.stream.Collectors
import java.util.UUID

class HttpDataRepoDAO(dataRepoInstanceName: String, dataRepoInstanceBasePath: String) extends DataRepoDAO {

  private val datareporow_id =
    new ColumnModel().name(DataRepoBigQuerySupport.datarepoRowIdColumn).datatype(TableDataType.STRING)

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(dataRepoInstanceBasePath)
    client.setAccessToken(accessToken)

    client
  }

  private def getRepositoryApi(accessToken: OAuth2BearerToken) =
    new RepositoryApi(getApiClient(accessToken.token))

  override def getInstanceName: String = dataRepoInstanceName

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel =
    // future enhancement: allow callers to specify the list of SnapshotRetrieveIncludeModel to retrieve
    addDataRepoRowId(getRepositoryApi(accessToken).retrieveSnapshot(snapshotId, java.util.Collections.emptyList()))

  // Snapshots-by-reference always have a datarepo_row_id, but that is not included in the model and should be
  private def addDataRepoRowId(snapshot: SnapshotModel): SnapshotModel = {
    Option(snapshot.getTables) foreach { tables =>
      snapshot.tables(tables.stream().map(t => t.addColumnsItem(datareporow_id)).collect(Collectors.toList()))
    }
    snapshot
  }
}
