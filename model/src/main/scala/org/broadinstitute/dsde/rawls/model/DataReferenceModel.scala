package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model.{DataReferenceDescription, DataReferenceList, DataRepoSnapshot}
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.JavaConverters._

case class DataReferenceName(value: String) extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName, snapshotId: String)
case class DataRepoSnapshotReference(referenceId: String, name: String, workspaceId: String, referenceType: Option[String], reference: Option[DataRepoSnapshot], cloningInstructions: String)
case class DataRepoSnapshotList(snapshots: List[DataRepoSnapshotReference])

object DataRepoSnapshotReference {
  def apply(ref: DataReferenceDescription): DataRepoSnapshotReference = {
    DataRepoSnapshotReference(ref.getReferenceId.toString, ref.getName, ref.getWorkspaceId.toString,
      Option(ref.getReferenceType.toString), Option(ref.getReference), ref.getCloningInstructions.toString)
  }
}

object DataRepoSnapshotList {
  def from(ref: DataReferenceList): DataRepoSnapshotList = {
    val buffer = for (r <- ref.getResources.asScala) yield DataRepoSnapshotReference(r)
    DataRepoSnapshotList(buffer.toList)
  }
}

object DataReferenceModelJsonSupport {
  implicit object DataRepoSnapshotFormat extends RootJsonFormat[DataRepoSnapshot] {
    val INSTANCE_NAME = "instanceName"
    val SNAPSHOT = "snapshot"

    def write(snap: DataRepoSnapshot) = JsObject(
      INSTANCE_NAME -> JsString(snap.getInstanceName),
      SNAPSHOT -> JsString(snap.getSnapshot)
    )

    def read(json: JsValue) = {
      json.asJsObject.getFields(INSTANCE_NAME, SNAPSHOT) match {
        case Seq(JsString(instanceName), JsString(snapshot)) =>
          new DataRepoSnapshot().instanceName(instanceName).snapshot(snapshot)
        case _ => throw new DeserializationException("DataRepoSnapshot expected")
      }
    }
  }

  implicit val DataReferenceNameFormat = ValueObjectFormat(DataReferenceName)
  implicit val NamedDataRepoSnapshotFormat = jsonFormat2(NamedDataRepoSnapshot)
  implicit val DataRepoSnapshotReferenceFormat = jsonFormat6(DataRepoSnapshotReference.apply)
  implicit val DataRepoSnapshotListFormat = jsonFormat1(DataRepoSnapshotList.apply)
}
