package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.client.ApiException
import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO

class MockDataRepoDAO extends DataRepoDAO {
  override def getInstanceName: String = "dr_instance_name"

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel =
    throw new ApiException(StatusCodes.NotFound.intValue, "mock DR has no snapshots")
}
