package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model.DataReferenceDescription
import bio.terra.workspace.model.DataReferenceList
import spray.json.DefaultJsonProtocol._
import scala.collection.JavaConverters._

case class DataRepoSnapshot(name: String, snapshotId: String)
case class DataRepoSnapshotReference(referenceId: String, name: String, workspaceId: String, referenceType: Option[String], reference: Option[String], cloningInstructions: String)
case class DataRepoSnapshotList(snapshots: List[DataRepoSnapshotReference])
case class EnumerateSnapshotRequestBody(offset: Int, limit: Int)


object DataRepoSnapshotReference {
  def apply(ref: DataReferenceDescription): DataRepoSnapshotReference = {
    DataRepoSnapshotReference(ref.getReferenceId.toString, ref.getName, ref.getWorkspaceId.toString,
      Option(ref.getReferenceType.toString), Option(ref.getReference), ref.getCloningInstructions.toString)
  }
}

object DataRepoSnapshotList {
  def apply(ref: DataReferenceList): DataRepoSnapshotList = {
    val buffer = for (r <- ref.getResources.asScala) yield DataRepoSnapshotReference(r)
    DataRepoSnapshotList(buffer.toList)
  }
}

object DataReferenceModelJsonSupport {
  implicit val DataRepoSnapshotFormat = jsonFormat2(DataRepoSnapshot)
  implicit val DataRepoSnapshotReferenceFormat = jsonFormat6(DataRepoSnapshotReference.apply)
  implicit val EnumerateSnapshotRequestBodyFormat = jsonFormat2(EnumerateSnapshotRequestBody)
}
