package org.broadinstitute.dsde.rawls.model

import com.wordnik.swagger.annotations.{ApiModel, ApiModelProperty}
import org.broadinstitute.dsde.rawls.VertexProperty
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import spray.json._

import scala.annotation.meta.field

trait Identifiable {
  def path : String
}

/**
 * Created by dvoet on 4/24/15.
 */
@ApiModel(value = "WorkspaceName")
case class WorkspaceName(
                      @(ApiModelProperty@field)(required = true, value = "The namespace the workspace belongs to")
                      namespace: String,
                      @(ApiModelProperty@field)(required = true, value = "The name of the workspace")
                      name: String) extends Identifiable {
  def path : String = "workspaces/" + namespace + "/" + name
}

@ApiModel(value = "Workspace")
case class Workspace (
                      @(ApiModelProperty@field)(required = true, value = "The namespace the workspace belongs to")
                      @(VertexProperty@field)
                      namespace: String,
                      @(ApiModelProperty@field)(required = true, value = "The name of the workspace")
                      @(VertexProperty@field)
                      name: String,
                      @(ApiModelProperty@field)(required = true, value = "The date the workspace was created in yyyy-MM-dd'T'HH:mm:ssZZ format")
                      @(VertexProperty@field)
                      createdDate: DateTime,
                      @(ApiModelProperty@field)(required = true, value = "The user who created the workspace")
                      @(VertexProperty@field)
                      createdBy: String,
                      @(ApiModelProperty@field)(required = true, value = "The attributes of the workspace")
                      attributes: Map[String, Attribute]
                      ) extends Identifiable {
  def path : String = "workspaces/" + namespace + "/" + name
}

@ApiModel(value = "Entity name")
case class EntityName(
                   @(ApiModelProperty@field)(required = true, value = "The name of the entity")
                   name: String)

@ApiModel(value = "Entity")
case class Entity(
                   @(ApiModelProperty@field)(required = true, value = "The name of the entity")
                   @(VertexProperty@field)
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "The type of the entity")
                   @(VertexProperty@field)
                   entityType: String,
                   @(ApiModelProperty@field)(required = true, value = "The attributes of the entity")
                   attributes: Map[String, Attribute],
                   @(ApiModelProperty@field)(required = true, value = "This entity's owning workspace")
                   workspaceName:WorkspaceName,
                   @(ApiModelProperty@field)(required = false, value = "This entity's Vault ID")
                   @(VertexProperty@field)
                   vaultId:String="") extends Identifiable {
  def path : String = workspaceName.path + "/entities/" + name
}

@ApiModel(value = "Method configuration name")
case class MethodConfigurationName(
                   @(ApiModelProperty@field)(required = true, value = "The name of the method configuration")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's owning namespace")
                   methodConfigurationNamespace: String,
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's owning workspace")
                   workspaceName: WorkspaceName
                   )

@ApiModel(value = "Method Configuration")
case class MethodConfiguration(
                   @(ApiModelProperty@field)(required = true, value = "The name of the method configuration")
                   @(VertexProperty@field)
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "The root entity type that the method will be running on")
                   @(VertexProperty@field)
                   rootEntityType: String,
                   @(ApiModelProperty@field)(required = true, value = "The namespace of method from method store")
                   @(VertexProperty@field)
                   methodNamespace: String,
                   @(ApiModelProperty@field)(required = true, value = "The name of method from method store")
                   @(VertexProperty@field)
                   methodName: String,
                   @(ApiModelProperty@field)(required = true, value = "The version of method from method store")
                   @(VertexProperty@field)
                   methodVersion: String,
                   @(ApiModelProperty@field)(required = false, value = "PreRequisites for the method")
                   prerequisites: Map[String, String],
                   @(ApiModelProperty@field)(required = true, value = "Inputs for the method")
                   inputs: Map[String, String],
                   @(ApiModelProperty@field)(required = false, value = "Outputs for the method")
                   outputs: Map[String, String],
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's owning workspace")
                   workspaceName:WorkspaceName,
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's namespace")
                   @(VertexProperty@field)
                   namespace: String) extends Identifiable {
  def path : String = workspaceName.path + "/methodConfigs/" + namespace + "/" + name
}
@ApiModel(value = "Method Configuration without inputs, outputs, or prerequisites")
case class MethodConfigurationShort(
                                @(ApiModelProperty@field)(required = true, value = "The name of the method configuration")
                                @(VertexProperty@field)
                                name: String,
                                @(ApiModelProperty@field)(required = true, value = "The root entity type that the method will be running on")
                                @(VertexProperty@field)
                                rootEntityType: String,
                                @(ApiModelProperty@field)(required = true, value = "The namespace of method from method store")
                                @(VertexProperty@field)
                                methodNamespace: String,
                                @(ApiModelProperty@field)(required = true, value = "The name of method from method store")
                                @(VertexProperty@field)
                                methodName: String,
                                @(ApiModelProperty@field)(required = true, value = "The version of method from method store")
                                @(VertexProperty@field)
                                methodVersion: String,
                                @(ApiModelProperty@field)(required = true, value = "This method configuration's owning workspace")
                                @(VertexProperty@field)
                                workspaceName:WorkspaceName,
                                @(ApiModelProperty@field)(required = true, value = "This method configuration's namespace")
                                @(VertexProperty@field)
                                namespace: String)
  
trait Attribute
trait AttributeValue extends Attribute
trait AttributeReference extends Attribute

case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case class AttributeValueList(val list: Seq[AttributeValue]) extends AttributeValue // recursive
case class AttributeReferenceList(val list: Seq[AttributeReferenceSingle]) extends AttributeReference // non-recursive
case class AttributeReferenceSingle(val entityType: String, val entityName: String) extends AttributeReference

object WorkspaceJsonSupport extends DefaultJsonProtocol {

  implicit object AttributeFormat extends RootJsonFormat[Attribute] {

    override def write(obj: Attribute): JsValue = obj match {
      case AttributeBoolean(b) => JsBoolean(b)
      case AttributeNumber(n) => JsNumber(n)
      case AttributeString(s) => JsString(s)
      case AttributeValueList(l) => JsArray(l.map(write(_)):_*)
      case AttributeReferenceList(l) => JsArray(l.map(write(_)):_*)
      case AttributeReferenceSingle(entityType, entityName) => JsObject(Map("entityType" -> JsString(entityType), "entityName" -> JsString(entityName)))
    }

    override def read(json: JsValue): Attribute = json match {
      case JsString(s) => AttributeString(s)
      case JsBoolean(b) => AttributeBoolean(b)
      case JsNumber(n) => AttributeNumber(n)
      case JsArray(a) => getAttributeList(a.map(read(_)))
      case JsObject(members) => AttributeReferenceSingle(members("entityType").asInstanceOf[JsString].value, members("entityName").asInstanceOf[JsString].value)
      case _ => throw new DeserializationException("unexpected json type")
    }

    def getAttributeList(s: Seq[Attribute]) = s match {
      case v: Seq[AttributeValue @unchecked] if (s.map(_.isInstanceOf[AttributeValue]).reduce(_&&_)) => AttributeValueList(v)
      case r: Seq[AttributeReferenceSingle @unchecked] if (s.map(_.isInstanceOf[AttributeReferenceSingle]).reduce(_&&_)) => AttributeReferenceList(r)
      case _ => throw new DeserializationException("illegal array type")
    }
  }

  implicit object DateJsonFormat extends RootJsonFormat[DateTime] {
    private val parserISO : DateTimeFormatter = {
      ISODateTimeFormat.dateTimeNoMillis()
    }

    override def write(obj: DateTime) = {
      JsString(parserISO.print(obj))
    }

    override def read(json: JsValue): DateTime = json match {
      case JsString(s) => parserISO.parseDateTime(s)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat5(Entity)

  implicit val WorkspaceFormat = jsonFormat5(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationFormat = jsonFormat10(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat7(MethodConfigurationShort)
}
