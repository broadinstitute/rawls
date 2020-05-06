package org.broadinstitute.dsde.rawls.model

import spray.json.DefaultJsonProtocol._

case class DataRepoSnapshot(name: String, snapshotId: String)
case class DataRepoSnapshotReference(referenceId: String, name: String, workspaceId: String, referenceType: Option[String], reference: Option[String], cloningInstructions: String)

object DataReferenceModelJsonSupport {
  implicit val DataRepoSnapshotFormat = jsonFormat2(DataRepoSnapshot)
  implicit val DataRepoSnapshotReferenceFormat = jsonFormat6(DataRepoSnapshotReference)
}

object DataReferenceType extends Enumeration {
  val DataRepoSnapshot = Value
}

object CloningInstructions extends Enumeration {
  val COPY_NOTHING, COPY_DEFINITION, COPY_RESOURCE, COPY_REFERENCE = Value
}