package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel

import java.util.UUID

trait DataRepoDAO {

  def getInstanceName: String

  def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel
}
