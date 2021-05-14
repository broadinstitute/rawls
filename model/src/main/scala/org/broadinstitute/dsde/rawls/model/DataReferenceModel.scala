package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model.{ResourceMetadata, _}
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat, enrichAny}

import scala.collection.JavaConverters._

case class DataReferenceName(value: String) extends ValueObject
case class DataReferenceDescriptionField(value: String = "") extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName, description: Option[DataReferenceDescriptionField], snapshotId: UUID)

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
    val DESCRIPTION = "description"
    val WORKSPACE_ID = "workspaceId"
    val REFERENCE_TYPE = "referenceType"
    val REFERENCE = "reference"
    val CLONING_INSTRUCTIONS = "cloningInstructions"

    override def write(description: DataReferenceDescription) = JsObject(
      REFERENCE_ID -> stringOrNull(description.getReferenceId),
      NAME -> stringOrNull(description.getName),
      DESCRIPTION -> stringOrNull(description.getDescription),
      WORKSPACE_ID -> stringOrNull(description.getWorkspaceId),
      REFERENCE_TYPE -> stringOrNull(description.getReferenceType),
      REFERENCE -> description.getReference.toJson,
      CLONING_INSTRUCTIONS -> stringOrNull(description.getCloningInstructions)
    )

    override def read(json: JsValue): DataReferenceDescription = {
      json.asJsObject.getFields(REFERENCE_ID, NAME, DESCRIPTION, WORKSPACE_ID, REFERENCE_TYPE, REFERENCE, CLONING_INSTRUCTIONS) match {
        case Seq(referenceId, JsString(name), JsString(description), workspaceId, JsString(referenceType), reference, JsString(cloningInstructions)) =>
          new DataReferenceDescription()
            .referenceId(referenceId.convertTo[UUID])
            .name(name)
            .description(description)
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


  implicit object ResourceMetadataFormat extends RootJsonFormat[ResourceMetadata] {
    val WORKSPACE_ID = "workspaceId"
    val RESOURCE_ID = "resourceId"
    val NAME = "name"
    val DESCRIPTION = "description"
    val RESOURCE_TYPE = "resourceType"
    val STEWARDSHIP_TYPE = "stewardshipType"
    val CLONING_INSTRUCTIONS = "cloningInstructions"

    override def write(metadata: ResourceMetadata)= JsObject(
      WORKSPACE_ID -> stringOrNull(metadata.getWorkspaceId),
      RESOURCE_ID -> stringOrNull(metadata.getResourceId),
      NAME -> stringOrNull(metadata.getName),
      DESCRIPTION -> stringOrNull(metadata.getDescription),
      RESOURCE_TYPE -> stringOrNull(metadata.getResourceType),
      STEWARDSHIP_TYPE -> stringOrNull(metadata.getStewardshipType),
      CLONING_INSTRUCTIONS -> stringOrNull(metadata.getCloningInstructions)
    )

    override def read(json: JsValue): ResourceMetadata = {
      json.asJsObject.getFields(WORKSPACE_ID, RESOURCE_ID, NAME, DESCRIPTION, RESOURCE_TYPE, STEWARDSHIP_TYPE, CLONING_INSTRUCTIONS) match {
        case Seq(workspaceId, resourceId, JsString(name), JsString(description), JsString(resourceType), JsString(stewardshipType), JsString(cloningInstructions)) =>
          new ResourceMetadata()
            .workspaceId(workspaceId.convertTo[UUID])
            .resourceId(resourceId.convertTo[UUID])
            .name(name)
            .description(description)
            .resourceType(ResourceType.fromValue(resourceType))
            .stewardshipType(StewardshipType.fromValue(stewardshipType))
            .cloningInstructions(CloningInstructionsEnum.fromValue(cloningInstructions))
      }
    }
  }

  implicit object DataRepoSnapshotAttributesFormat extends RootJsonFormat[DataRepoSnapshotAttributes] {
    val INSTANCE_NAME = "instanceName"
    val SNAPSHOT = "snapshot"

    override def write(attributes: DataRepoSnapshotAttributes) = JsObject(
      INSTANCE_NAME -> stringOrNull(attributes.getInstanceName),
      SNAPSHOT -> stringOrNull(attributes.getSnapshot)
    )

    override def read(json: JsValue): DataRepoSnapshotAttributes = {
      json.asJsObject.getFields(INSTANCE_NAME, SNAPSHOT) match {
        case Seq(JsString(instanceName), JsString(snapshot)) =>
          new DataRepoSnapshotAttributes()
            .instanceName(instanceName)
            .snapshot(snapshot)
      }
    }
  }

  implicit object DataRepoSnapshotResourceFormat extends RootJsonFormat[DataRepoSnapshotResource] {
    val METADATA = "metadata"
    val ATTRIBUTES = "attributes"

    override def write(resource: DataRepoSnapshotResource) = JsObject(
      METADATA -> ResourceMetadataFormat.write(resource.getMetadata),
      ATTRIBUTES -> DataRepoSnapshotAttributesFormat.write(resource.getAttributes)
    )

    override def read(json: JsValue): DataRepoSnapshotResource = {
      val fields = json.asJsObject.getFields(METADATA, ATTRIBUTES)
      fields match {
        case Seq(metadata @ JsObject(_), attributes @ JsObject(_)) =>
          val res = new DataRepoSnapshotResource()
            .metadata(metadata.convertTo[ResourceMetadata])
            .attributes(attributes.convertTo[DataRepoSnapshotAttributes])
          res
        case _ => throw DeserializationException("DataRepoSnapshotResource expected")
      }
    }
  }

  implicit object UpdateDataReferenceRequestFormat extends RootJsonFormat[UpdateDataReferenceRequestBody] {
    val NAME = "name"
    val DESCRIPTION = "description"

    override def write(request: UpdateDataReferenceRequestBody) = JsObject(
      NAME -> stringOrNull(request.getName),
      DESCRIPTION -> stringOrNull(request.getDescription),
    )

    override def read(json: JsValue): UpdateDataReferenceRequestBody = {
      val jsObject = json.asJsObject

      def getOptionalStringField(fieldName: String): Option[String] = {
        jsObject.fields.get(fieldName) match {
          case Some(s:JsString) => Option(s.value)
          case _ => None
        }
      }

      jsObject.getFields(NAME, DESCRIPTION) match {
        case Seq() => throw DeserializationException("UpdateDataReferenceRequestBody expected")
        case _ => // both fields are optional, as long as one is present we can proceed
          val updateRequest = new UpdateDataReferenceRequestBody()

          getOptionalStringField(NAME).map(updateRequest.name)
          getOptionalStringField(DESCRIPTION).map(updateRequest.description)

          updateRequest
      }
    }
  }


  implicit object ResourceAttributesUnionFormat extends RootJsonFormat[ResourceAttributesUnion] {
    val GCP_DATA_REPO_SNAPSHOT = "gcpDataRepoSnapshot"

    override def write(attributesUnion: ResourceAttributesUnion) = JsObject(
      GCP_DATA_REPO_SNAPSHOT -> DataRepoSnapshotAttributesFormat.write(attributesUnion.getGcpDataRepoSnapshot)
    )

    override def read(json: JsValue): ResourceAttributesUnion = {
      json.asJsObject.getFields(GCP_DATA_REPO_SNAPSHOT) match {
        case Seq(gcpDataRepoSnapshot @ JsObject(_)) =>
          new ResourceAttributesUnion().gcpDataRepoSnapshot(gcpDataRepoSnapshot.convertTo[DataRepoSnapshotAttributes])
        case _ => throw DeserializationException("ResourceAttributesUnion expected")
      }
    }
  }

  implicit object ResourceDescriptionFormat extends RootJsonFormat[ResourceDescription] {
    val METADATA = "metadata"
    val RESOURCE_ATTRIBUTES = "resourceAttributes"

    override def write(description: ResourceDescription) = JsObject(
      METADATA -> ResourceMetadataFormat.write(description.getMetadata),
      RESOURCE_ATTRIBUTES -> ResourceAttributesUnionFormat.write(description.getResourceAttributes)
    )

    override def read(json: JsValue): ResourceDescription = {
      json.asJsObject.getFields(METADATA, RESOURCE_ATTRIBUTES) match {
        case Seq(metadata, resourceAttributes) =>
          new ResourceDescription()
            .metadata(metadata.convertTo[ResourceMetadata])
            .resourceAttributes(resourceAttributes.convertTo[ResourceAttributesUnion])
      }
    }
  }

  implicit object ResourceListFormat extends RootJsonFormat[ResourceList] {
    val RESOURCES = "resources"

    override def write(refList: ResourceList) = JsObject(
      RESOURCES -> refList.getResources.asScala.toList.toJson
    )

    override def read(json: JsValue): ResourceList = {
      json.asJsObject.getFields(RESOURCES) match {
        case Seq(JsArray(resources)) =>
          new ResourceList().resources(resources.map(_.convertTo[ResourceDescription]).asJava)
        case _ => throw DeserializationException("ResourceList expected")
      }
    }
  }





  implicit val DataReferenceNameFormat = ValueObjectFormat(DataReferenceName)
  implicit val dataReferenceDescriptionFieldFormat = ValueObjectFormat(DataReferenceDescriptionField)
  implicit val NamedDataRepoSnapshotFormat = jsonFormat3(NamedDataRepoSnapshot)
}
