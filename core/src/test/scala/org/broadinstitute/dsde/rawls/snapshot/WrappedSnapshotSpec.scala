package org.broadinstitute.dsde.rawls.snapshot

import bio.terra.datarepo.model.{CloudPlatform, DatasetSummaryModel, SnapshotModel, SnapshotSourceModel}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class WrappedSnapshotSpec extends AnyFlatSpecLike with Matchers with TestDriverComponent {

  behavior of "isProtected"

  it should "be true when secureMonitoringEnabled is true" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().secureMonitoringEnabled(true))
          )
      )

    (new WrappedSnapshot(snapshot).isProtected) shouldBe true
  }

  it should "be false when secureMonitoringEnabled is false" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().secureMonitoringEnabled(false))
          )
      )

    (new WrappedSnapshot(snapshot).isProtected) shouldBe false
  }

  it should "throw when secureMonitoringEnabled is null" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().secureMonitoringEnabled(null))
          )
      )
    val thrown = intercept[RawlsException] {
      new WrappedSnapshot(snapshot).isProtected
    }

    thrown.getMessage contains "had null value for secure monitoring"
  }

  behavior of "platform"

  it should "be GCP when snapshot cloudPlatform is GCP" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().cloudPlatform(CloudPlatform.GCP))
          )
      )

    (new WrappedSnapshot(snapshot).platform) shouldBe CloudPlatform.GCP
  }

  it should "be AZURE when snapshot cloudPlatform is AZURE" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().cloudPlatform(CloudPlatform.AZURE))
          )
      )

    (new WrappedSnapshot(snapshot).platform) shouldBe CloudPlatform.AZURE
  }

  it should "throw when snapshot cloudPlatform is null" in {
    val snapshot = new SnapshotModel()
      .source(
        java.util.List
          .of(
            new SnapshotSourceModel()
              .dataset(new DatasetSummaryModel().cloudPlatform(null))
          )
      )

    val thrown = intercept[RawlsException] {
      new WrappedSnapshot(snapshot).platform
    }

    thrown.getMessage contains "had null value for cloud platform"
  }
}
