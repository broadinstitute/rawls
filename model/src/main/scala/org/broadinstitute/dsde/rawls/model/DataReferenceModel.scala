package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsObject, JsString, JsValue, RootJsonFormat, enrichAny}

import scala.collection.JavaConverters._

case class DataReferenceName(value: String) extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName, snapshotId: String)

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

  // Only handling supported fields for now, resourceDescription and credentialId aren't used currently
  implicit object DataReferenceDescriptionFormat extends RootJsonFormat[DataReferenceDescription] {
    val REFERENCE_ID = "referenceId"
    val NAME = "name"
    val WORKSPACE_ID = "workspaceId"
    val REFERENCE_TYPE = "referenceType"
    val REFERENCE = "reference"
    val CLONING_INSTRUCTIONS = "cloningInstructions"

    def write(description: DataReferenceDescription) = JsObject(
      REFERENCE_ID -> JsString(description.getReferenceId.toString),
      NAME -> JsString(description.getName),
      WORKSPACE_ID -> JsString(description.getWorkspaceId.toString),
      REFERENCE_TYPE -> JsString(description.getReferenceType.toString),
      REFERENCE -> DataRepoSnapshotFormat.write(description.getReference),
      CLONING_INSTRUCTIONS -> JsString(description.getCloningInstructions.toString)
    )

    def read(json: JsValue): DataReferenceDescription = {
      json.asJsObject.getFields(REFERENCE_ID, NAME, WORKSPACE_ID, REFERENCE_TYPE, REFERENCE, CLONING_INSTRUCTIONS) match {
        case Seq(JsString(referenceId), JsString(name), JsString(workspaceId), JsString(referenceType), reference, JsString(cloningInstructions)) =>
          new DataReferenceDescription()
            .referenceId(UUID.fromString(referenceId))
            .name(name)
            .workspaceId(UUID.fromString(workspaceId))
            .referenceType(ReferenceTypeEnum.fromValue(referenceType))
            .reference(DataRepoSnapshotFormat.read(reference))
            .cloningInstructions(CloningInstructionsEnum.fromValue(cloningInstructions))
        case _ => throw new DeserializationException("DataReferenceDescription expected")
      }
    }
  }

  implicit object DataReferenceListFormat extends RootJsonFormat[DataReferenceList] {
    val RESOURCES = "resources"

    def write(refList: DataReferenceList) = JsObject(
      RESOURCES -> refList.getResources.asScala.toList.map(DataReferenceDescriptionFormat.write).toJson
    )

    def read(json: JsValue): DataReferenceList = {
      json.asJsObject.getFields(RESOURCES) match {
        case Seq(JsArray(resources)) =>
          new DataReferenceList().resources(resources.map(DataReferenceDescriptionFormat.read).asJava)
        case _ => throw new DeserializationException("DataReferenceList expected")
      }
    }
  }

  implicit val DataReferenceNameFormat = ValueObjectFormat(DataReferenceName)
  implicit val NamedDataRepoSnapshotFormat = jsonFormat2(NamedDataRepoSnapshot)
}
