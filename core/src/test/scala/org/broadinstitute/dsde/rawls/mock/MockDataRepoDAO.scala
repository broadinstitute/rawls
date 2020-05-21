package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO

class MockDataRepoDAO extends DataRepoDAO {
  override def getBaseURL: String = ???

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = ???
}
