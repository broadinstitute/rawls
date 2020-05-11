package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model.DataReferenceDescription
import spray.json.DefaultJsonProtocol._

case class DataRepoSnapshot(name: String, snapshotId: String)
case class DataRepoSnapshotReference(referenceId: String, name: String, workspaceId: String, referenceType: Option[String], reference: Option[String], cloningInstructions: String)

object DataRepoSnapshotReference {
  def apply(ref: DataReferenceDescription): DataRepoSnapshotReference = {
    DataRepoSnapshotReference(ref.getReferenceId.toString, ref.getName, ref.getWorkspaceId.toString,
      Option(ref.getReferenceType.toString), Option(ref.getReference), ref.getCloningInstructions.toString)
  }
}

object DataReferenceModelJsonSupport {
  implicit val DataRepoSnapshotFormat = jsonFormat2(DataRepoSnapshot)
  implicit val DataRepoSnapshotReferenceFormat = jsonFormat6(DataRepoSnapshotReference)
}
