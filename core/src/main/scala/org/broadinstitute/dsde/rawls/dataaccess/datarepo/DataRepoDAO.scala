package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.SnapshotModel

trait DataRepoDAO {

  def getBaseURL: String

  def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel
}
