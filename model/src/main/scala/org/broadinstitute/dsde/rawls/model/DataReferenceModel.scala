package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat, enrichAny}

import scala.collection.JavaConverters._

case class DataReferenceName(value: String) extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName, snapshotId: String)

object DataReferenceModelJsonSupport extends JsonSupport {
  def stringOrNull(in: Any): JsValue = Option(in) match {
    case None => JsNull
    case Some(str: String) => JsString(str)
    case Some(notStr) => JsString(notStr.toString)
  }

  implicit object DataRepoSnapshotFormat extends RootJsonFormat[DataRepoSnapshot] {
    val INSTANCE_NAME = "instanceName"
    val SNAPSHOT = "snapshot"

    override def write(snap: DataRepoSnapshot) = JsObject(
      INSTANCE_NAME -> stringOrNull(snap.getInstanceName),
      SNAPSHOT -> stringOrNull(snap.getSnapshot)
    )

    override def read(json: JsValue) = {
      json.asJsObject.getFields(INSTANCE_NAME, SNAPSHOT) match {
        case Seq(JsString(instanceName), JsString(snapshot)) =>
          new DataRepoSnapshot().instanceName(instanceName).snapshot(snapshot)
        case _ => throw DeserializationException("DataRepoSnapshot expected")
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

    override def write(description: DataReferenceDescription) = JsObject(
      REFERENCE_ID -> stringOrNull(description.getReferenceId),
      NAME -> stringOrNull(description.getName),
      WORKSPACE_ID -> stringOrNull(description.getWorkspaceId),
      REFERENCE_TYPE -> stringOrNull(description.getReferenceType),
      REFERENCE -> description.getReference.toJson,
      CLONING_INSTRUCTIONS -> stringOrNull(description.getCloningInstructions)
    )

    override def read(json: JsValue): DataReferenceDescription = {
      json.asJsObject.getFields(REFERENCE_ID, NAME, WORKSPACE_ID, REFERENCE_TYPE, REFERENCE, CLONING_INSTRUCTIONS) match {
        case Seq(referenceId, JsString(name), workspaceId, JsString(referenceType), reference, JsString(cloningInstructions)) =>
          new DataReferenceDescription()
            .referenceId(referenceId.convertTo[UUID])
            .name(name)
            .workspaceId(workspaceId.convertTo[UUID])
            .referenceType(ReferenceTypeEnum.fromValue(referenceType))
            .reference(reference.convertTo[DataRepoSnapshot])
            .cloningInstructions(CloningInstructionsEnum.fromValue(cloningInstructions))
        case _ => throw DeserializationException("DataReferenceDescription expected")
      }
    }
  }

  implicit object DataReferenceListFormat extends RootJsonFormat[DataReferenceList] {
    val RESOURCES = "resources"

    override def write(refList: DataReferenceList) = JsObject(
      RESOURCES -> refList.getResources.asScala.toList.toJson
    )

    override def read(json: JsValue): DataReferenceList = {
      json.asJsObject.getFields(RESOURCES) match {
        case Seq(JsArray(resources)) =>
          new DataReferenceList().resources(resources.map(_.convertTo[DataReferenceDescription]).asJava)
        case _ => throw DeserializationException("DataReferenceList expected")
      }
    }
  }

  implicit val DataReferenceNameFormat = ValueObjectFormat(DataReferenceName)
  implicit val NamedDataRepoSnapshotFormat = jsonFormat2(NamedDataRepoSnapshot)
}
