package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{enrichAny, DeserializationException, JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat}

import java.util.UUID
import scala.jdk.CollectionConverters._

case class DataReferenceName(value: String) extends ValueObject
case class DataReferenceDescriptionField(value: String = "") extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName,
                                 description: Option[DataReferenceDescriptionField],
                                 snapshotId: UUID
)
case class SnapshotListResponse(gcpDataRepoSnapshots: Seq[DataRepoSnapshotResource])

object DataReferenceModelJsonSupport extends JsonSupport {
  def stringOrNull(in: Any): JsValue = Option(in) match {
    case None              => JsNull
    case Some(str: String) => JsString(str)
    case Some(notStr)      => JsString(notStr.toString)
  }

  implicit object ResourceMetadataFormat extends RootJsonFormat[ResourceMetadata] {
    val WORKSPACE_ID = "workspaceId"
    val RESOURCE_ID = "resourceId"
    val NAME = "name"
    val DESCRIPTION = "description"
    val RESOURCE_TYPE = "resourceType"
    val STEWARDSHIP_TYPE = "stewardshipType"
    val CLONING_INSTRUCTIONS = "cloningInstructions"

    override def write(metadata: ResourceMetadata) = JsObject(
      WORKSPACE_ID -> stringOrNull(metadata.getWorkspaceId),
      RESOURCE_ID -> stringOrNull(metadata.getResourceId),
      NAME -> stringOrNull(metadata.getName),
      DESCRIPTION -> stringOrNull(metadata.getDescription),
      RESOURCE_TYPE -> stringOrNull(metadata.getResourceType),
      STEWARDSHIP_TYPE -> stringOrNull(metadata.getStewardshipType),
      CLONING_INSTRUCTIONS -> stringOrNull(metadata.getCloningInstructions)
    )

    override def read(json: JsValue): ResourceMetadata =
      json.asJsObject.getFields(WORKSPACE_ID,
                                RESOURCE_ID,
                                NAME,
                                DESCRIPTION,
                                RESOURCE_TYPE,
                                STEWARDSHIP_TYPE,
                                CLONING_INSTRUCTIONS
      ) match {
        case Seq(workspaceId,
                 resourceId,
                 JsString(name),
                 JsString(description),
                 JsString(resourceType),
                 JsString(stewardshipType),
                 JsString(cloningInstructions)
            ) =>
          new ResourceMetadata()
            .workspaceId(workspaceId.convertTo[UUID])
            .resourceId(resourceId.convertTo[UUID])
            .name(name)
            .description(description)
            .resourceType(ResourceType.fromValue(resourceType))
            .stewardshipType(StewardshipType.fromValue(stewardshipType))
            .cloningInstructions(CloningInstructionsEnum.fromValue(cloningInstructions))
        case _ => throw DeserializationException("ResourceMetadata expected")
      }
  }

  implicit object DataRepoSnapshotAttributesFormat extends RootJsonFormat[DataRepoSnapshotAttributes] {
    val INSTANCE_NAME = "instanceName"
    val SNAPSHOT = "snapshot"

    override def write(attributes: DataRepoSnapshotAttributes) = JsObject(
      INSTANCE_NAME -> stringOrNull(attributes.getInstanceName),
      SNAPSHOT -> stringOrNull(attributes.getSnapshot)
    )

    override def read(json: JsValue): DataRepoSnapshotAttributes =
      json.asJsObject.getFields(INSTANCE_NAME, SNAPSHOT) match {
        case Seq(JsString(instanceName), JsString(snapshot)) =>
          new DataRepoSnapshotAttributes()
            .instanceName(instanceName)
            .snapshot(snapshot)
        case _ => throw DeserializationException("DataRepoSnapshotAttributes expected")
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

  implicit object UpdateDataRepoSnapshotReferenceRequestBodyFormat
      extends RootJsonFormat[UpdateDataRepoSnapshotReferenceRequestBody] {
    val NAME = "name"
    val DESCRIPTION = "description"
    val INSTANCE_NAME = "instanceName"
    val SNAPSHOT = "snapshot"

    override def write(request: UpdateDataRepoSnapshotReferenceRequestBody) = JsObject(
      NAME -> stringOrNull(request.getName),
      DESCRIPTION -> stringOrNull(request.getDescription),
      INSTANCE_NAME -> stringOrNull(request.getInstanceName),
      SNAPSHOT -> stringOrNull(request.getSnapshot)
    )

    override def read(json: JsValue): UpdateDataRepoSnapshotReferenceRequestBody = {
      val jsObject = json.asJsObject

      def getOptionalStringField(fieldName: String): Option[String] =
        jsObject.fields.get(fieldName) match {
          case Some(s: JsString) => Option(s.value)
          case _                 => None
        }

      jsObject.getFields(NAME, DESCRIPTION, INSTANCE_NAME, SNAPSHOT) match {
        case Seq() => throw DeserializationException("UpdateDataRepoSnapshotReferenceRequestBody expected")
        case _ => // all fields are optional, as long as one is present we can proceed
          val updateRequest = new UpdateDataRepoSnapshotReferenceRequestBody()

          getOptionalStringField(NAME).map(updateRequest.name)
          getOptionalStringField(DESCRIPTION).map(updateRequest.description)
          getOptionalStringField(INSTANCE_NAME).map(updateRequest.instanceName)
          getOptionalStringField(SNAPSHOT).map(updateRequest.snapshot)

          updateRequest
      }
    }
  }

  implicit object ResourceAttributesUnionFormat extends RootJsonFormat[ResourceAttributesUnion] {
    val GCP_DATA_REPO_SNAPSHOT = "gcpDataRepoSnapshot"

    override def write(attributesUnion: ResourceAttributesUnion) = JsObject(
      GCP_DATA_REPO_SNAPSHOT -> DataRepoSnapshotAttributesFormat.write(attributesUnion.getGcpDataRepoSnapshot)
    )

    override def read(json: JsValue): ResourceAttributesUnion =
      json.asJsObject.getFields(GCP_DATA_REPO_SNAPSHOT) match {
        case Seq(gcpDataRepoSnapshot @ JsObject(_)) =>
          new ResourceAttributesUnion().gcpDataRepoSnapshot(gcpDataRepoSnapshot.convertTo[DataRepoSnapshotAttributes])
        case _ => throw DeserializationException("ResourceAttributesUnion expected")
      }
  }

  implicit object ResourceDescriptionFormat extends RootJsonFormat[ResourceDescription] {
    val METADATA = "metadata"
    val RESOURCE_ATTRIBUTES = "resourceAttributes"

    override def write(description: ResourceDescription) = JsObject(
      METADATA -> ResourceMetadataFormat.write(description.getMetadata),
      RESOURCE_ATTRIBUTES -> ResourceAttributesUnionFormat.write(description.getResourceAttributes)
    )

    override def read(json: JsValue): ResourceDescription =
      json.asJsObject.getFields(METADATA, RESOURCE_ATTRIBUTES) match {
        case Seq(metadata, resourceAttributes) =>
          new ResourceDescription()
            .metadata(metadata.convertTo[ResourceMetadata])
            .resourceAttributes(resourceAttributes.convertTo[ResourceAttributesUnion])
      }
  }

  implicit object ResourceListFormat extends RootJsonFormat[ResourceList] {
    val RESOURCES = "resources"

    override def write(refList: ResourceList) = JsObject(
      RESOURCES -> refList.getResources.asScala.toList.toJson
    )

    override def read(json: JsValue): ResourceList =
      json.asJsObject.getFields(RESOURCES) match {
        case Seq(JsArray(resources)) =>
          new ResourceList().resources(resources.map(_.convertTo[ResourceDescription]).asJava)
        case _ => throw DeserializationException("ResourceList expected")
      }
  }

  implicit val DataReferenceNameFormat: ValueObjectFormat[DataReferenceName] = ValueObjectFormat(DataReferenceName)
  implicit val dataReferenceDescriptionFieldFormat: ValueObjectFormat[DataReferenceDescriptionField] =
    ValueObjectFormat(DataReferenceDescriptionField)
  implicit val NamedDataRepoSnapshotFormat: RootJsonFormat[NamedDataRepoSnapshot] = jsonFormat3(NamedDataRepoSnapshot)
  implicit val SnapshotListResponseFormat: RootJsonFormat[SnapshotListResponse] = jsonFormat1(SnapshotListResponse)
}
