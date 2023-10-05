package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.model.{CloudPlatform, DatasetSummaryModel, SnapshotModel, SnapshotSourceModel}
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO

import java.util.UUID

class MockDataRepoDAO(instanceName: String) extends DataRepoDAO {
  override def getInstanceName: String = instanceName

  override def getSnapshot(snapshotId: UUID, accessToken: OAuth2BearerToken): SnapshotModel =
    new SnapshotModel()
      .id(snapshotId)
      .name("snapshotName")
      .description("snapshotDescription")
      .source(
        java.util.List.of(new SnapshotSourceModel().dataset(new DatasetSummaryModel().cloudPlatform(CloudPlatform.GCP)))
      )

}
