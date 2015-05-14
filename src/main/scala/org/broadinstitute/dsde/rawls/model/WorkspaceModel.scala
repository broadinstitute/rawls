package org.broadinstitute.dsde.rawls.model

import com.wordnik.swagger.annotations.{ApiModel, ApiModelProperty}
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

@ApiModel(value = "WorkspaceShort")
case class WorkspaceShort(
                      @(ApiModelProperty@field)(required = true, value = "The namespace the workspace belongs to")
                      namespace: String,
                      @(ApiModelProperty@field)(required = true, value = "The name of the workspace")
                      name: String,
                      @(ApiModelProperty@field)(required = true, value = "The date the workspace was created in yyyy-MM-dd'T'HH:mm:ssZZ format")
                      createdDate: DateTime,
                      @(ApiModelProperty@field)(required = true, value = "The user who created the workspace")
                      createdBy: String) extends Identifiable {
  def path : String = "workspaces/" + namespace + "/" + name
}

@ApiModel(value = "Workspace")
case class Workspace (
                      @(ApiModelProperty@field)(required = true, value = "The namespace the workspace belongs to")
                      namespace: String,
                      @(ApiModelProperty@field)(required = true, value = "The name of the workspace")
                      name: String,
                      @(ApiModelProperty@field)(required = true, value = "The date the workspace was created in yyyy-MM-dd'T'HH:mm:ssZZ format")
                      createdDate: DateTime,
                      @(ApiModelProperty@field)(required = true, value = "The user who created the workspace")
                      createdBy: String,
                      @(ApiModelProperty@field)(required = true, value = "Entities in the workspace, first key: entity type, second key: entity name")
                      entities: Map[String, Map[String, Entity]] ) extends Identifiable {
  def path : String = "workspaces/" + namespace + "/" + name
}

@ApiModel(value = "Entity name")
case class EntityName(
                   @(ApiModelProperty@field)(required = true, value = "The name of the entity")
                   name: String)

@ApiModel(value = "Entity")
case class Entity(
                   @(ApiModelProperty@field)(required = true, value = "The name of the entity")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "The type of the entity")
                   entityType: String,
                   @(ApiModelProperty@field)(required = true, value = "The attributes of the entity")
                   attributes: Map[String, Attribute],
                   @(ApiModelProperty@field)(required = true, value = "This entity's owning workspace")
                   workspaceName:WorkspaceName ) extends Identifiable {
  def path : String = workspaceName.path + "/entities/" + name
}

@ApiModel(value = "Task configuration name")
case class TaskConfigurationName(
                   @(ApiModelProperty@field)(required = true, value = "The name of the task configuration")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "This task configuration's owning workspace")
                   workspaceName: WorkspaceName
                   )

@ApiModel(value = "Task")
case class Task(
                   @(ApiModelProperty@field)(required = true, value = "The name of the task")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "The namespace of the task")
                   nameSpace: String,
                   @(ApiModelProperty@field)(required = true, value = "The version of the task")
                   version: String
                 )
@ApiModel(value = "Task Configuration")
case class TaskConfiguration(
                   @(ApiModelProperty@field)(required = true, value = "The name of the task configuration")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "The root entity type that the task will be running on")
                   rootEntityType: String,
                   @(ApiModelProperty@field)(required = true, value = "The task LSID")
                   task: Task,
                   @(ApiModelProperty@field)(required = true, value = "Inputs for the task")
                   inputs: Map[String, String],
                   @(ApiModelProperty@field)(required = false, value = "Outputs for the task")
                   outputs: Map[String, String],
                   @(ApiModelProperty@field)(required = true, value = "This task configuration's owning workspace")
                   workspaceName:WorkspaceName) extends Identifiable {
  def path : String = workspaceName.path + "/taskConfigs/" + name
}

trait Attribute
trait AttributeValue extends Attribute
trait AttributeReference extends Attribute

case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case class AttributeValueList(val list: Seq[AttributeValue]) extends AttributeValue // recursive
case class AttributeReferenceList(val list: Seq[AttributeReferenceSingle]) extends AttributeReference // non-recursive

case class AttributeReferenceSingle(val entityType: String, val entityName: String) extends AttributeReference {
  def resolve(context: Workspace): Option[Entity] = {
    context.entities.getOrElse(entityType, Map.empty).get(entityName)
  }
}

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
      case v: Seq[AttributeValue] if (s.map(_.isInstanceOf[AttributeValue]).reduce(_&&_)) => AttributeValueList(v)
      case r: Seq[AttributeReferenceSingle] if (s.map(_.isInstanceOf[AttributeReferenceSingle]).reduce(_&&_)) => AttributeReferenceList(r)
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

  implicit val WorkspaceShortFormat = jsonFormat4(WorkspaceShort)

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat4(Entity)

  implicit val WorkspaceFormat = jsonFormat5(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val TaskConfigurationNameFormat = jsonFormat2(TaskConfigurationName)

  implicit val TaskFormat = jsonFormat3(Task)

  implicit val TaskConfigurationFormat = jsonFormat6(TaskConfiguration)
}
