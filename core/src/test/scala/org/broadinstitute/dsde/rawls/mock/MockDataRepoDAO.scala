package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO

class MockDataRepoDAO(instanceName: String) extends DataRepoDAO {
  override def getInstanceName: String = instanceName

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel = {
    val snap = new SnapshotModel()
    snap.id(snapshotId.toString)
    snap.name("snapshotName")
    snap.description("snapshotDescription")

    snap

  }
}
