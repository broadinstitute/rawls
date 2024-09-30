package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.{PolicyResponse, SnapshotModel}

import java.util.UUID

trait DataRepoDAO {

  def getInstanceName: String

  def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel

  def retrieveSnapshotPolicies(snapshotId: UUID, accessToken: OAuth2BearerToken): PolicyResponse

  def removeSnapshotPolicy(snapshotId: UUID, member: String, accessToken: OAuth2BearerToken)
}
