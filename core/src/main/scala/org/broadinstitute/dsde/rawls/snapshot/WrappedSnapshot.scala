package org.broadinstitute.dsde.rawls.snapshot

import bio.terra.datarepo.model.{CloudPlatform, DatasetSummaryModel, SnapshotModel}
import org.broadinstitute.dsde.rawls.RawlsException

import scala.jdk.CollectionConverters.CollectionHasAsScala

// A basic wrapper class to simplify interaction with the SnapshotModel from TDR
class WrappedSnapshot(snapshot: SnapshotModel) {

  // While getSource returns a list of SnapshotSourceModels, this is an artifact and we can make an assumption that
  // a given snapshot will have exactly one source that will always have a dataset.  Violations of this assumption
  // indicate a bug, in which case, crash and burn.
  private def snapshotDataset(): DatasetSummaryModel = getOnlyElement(snapshot.getSource.asScala.toList).getDataset

  private def getOnlyElement[A](list: List[A]): A = list match {
    case onlyElement :: Nil => onlyElement
    case _                  => throw new NoSuchElementException(s"Expected exactly one element, but found ${list.size}")
  }

  def isProtected: Boolean = Option(snapshotDataset().isSecureMonitoringEnabled)
    .getOrElse(
      throw new RawlsException(
        s"Snapshot ${snapshot.getId}'s DatasetSummaryModel had null value for secure monitoring; cannot continue"
      )
    )

  def platform: CloudPlatform = Option(snapshotDataset().getCloudPlatform)
    .getOrElse(
      throw new RawlsException(
        s"Snapshot ${snapshot.getId}'s DatasetSummaryModel had null value for cloud platform; cannot continue"
      )
    )
}
