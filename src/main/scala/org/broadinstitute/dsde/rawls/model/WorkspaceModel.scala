package org.broadinstitute.dsde.rawls.model

import com.wordnik.swagger.annotations.{ApiModel, ApiModelProperty}
import org.broadinstitute.dsde.rawls.VertexProperty
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import spray.json._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.annotation.meta.field

trait Identifiable {
  def path : String
}

trait Attributable extends Identifiable {
  def attributes: Map[String, Attribute]
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
                      @(ApiModelProperty@field)(required = true, value = "The date the workspace was created in yyyy-MM-dd'T'HH:mm:ss.SSSZZ format")
                      @(VertexProperty@field)
                      createdDate: DateTime,
                      @(ApiModelProperty@field)(required = true, value = "The user who created the workspace")
                      @(VertexProperty@field)
                      createdBy: String,
                      @(ApiModelProperty@field)(required = true, value = "The attributes of the workspace")
                      attributes: Map[String, Attribute]
                      ) extends Identifiable with Attributable {
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
                   vaultId:String="") extends Identifiable with Attributable {
  def path : String = workspaceName.path + "/entities/" + name
}

@ApiModel(value = "Method configuration name")
case class MethodConfigurationName(
                   @(ApiModelProperty@field)(required = true, value = "The name of the method configuration")
                   name: String,
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's owning namespace")
                   namespace: String,
                   @(ApiModelProperty@field)(required = true, value = "This method configuration's owning workspace")
                   workspaceName: WorkspaceName
                   )

@ApiModel(value = "Method configuration name pair")
case class MethodConfigurationNamePair(
                   @(ApiModelProperty@field)(required = true, value = "The source of a copied method configuration")
                   source: MethodConfigurationName,
                   @(ApiModelProperty@field)(required = true, value = "The destination for a copied method configuration")
                   destination: MethodConfigurationName
                   )

@ApiModel(value = "Entity copy definition")
case class EntityCopyDefinition(
                   @(ApiModelProperty@field)(required = true, value = "The source workspace of the entities")
                   sourceWorkspace: WorkspaceName,
                   @(ApiModelProperty@field)(required = true, value = "The destination workspace for the entities")
                   destinationWorkspace: WorkspaceName,
                   @(ApiModelProperty@field)(required = true, value = "Type of top level entities to copy")
                   entityType: String,
                   @(ApiModelProperty@field)(required = true, value = "Names of entities to copy")
                   entityNames: Seq[String]
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

@ApiModel(value = "Method repository configuration query")
case class MethodRepoConfigurationQuery(
                                         @(ApiModelProperty@field)(required = true, value = "Method Repository Namespace")
                                         methodRepoNamespace: String,
                                         @(ApiModelProperty@field)(required = true, value = "Method Repository Name")
                                         methodRepoName: String,
                                         @(ApiModelProperty@field)(required = true, value = "Method Repository Snapshot ID")
                                         methodRepoSnapshotId: String,
                                         @(ApiModelProperty@field)(required = true, value = "The destination for a copied method configuration")
                                         destination: MethodConfigurationName
                                         )

case class ConflictingEntities(conflicts: Seq[String])

trait Attribute
trait AttributeValue extends Attribute
trait AttributeReference extends Attribute

case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case class AttributeValueList(val list: Seq[AttributeValue]) extends AttributeValue // recursive
case class AttributeReferenceList(val list: Seq[AttributeReferenceSingle]) extends AttributeReference // non-recursive
case class AttributeReferenceSingle(val entityType: String, val entityName: String) extends AttributeReference

object AttributeConversions {
  // need to do some casting to conform to this list: http://orientdb.com/docs/last/Types.html
  def attributeToProperty(att: AttributeValue): Any = att match {
    case AttributeBoolean(b) => b
    case AttributeNumber(n) => n.bigDecimal
    case AttributeString(s) => s
    case AttributeValueList(l) => l.map(attributeToProperty(_)).asJava
    case _ => throw new IllegalArgumentException("Cannot serialize " + att + " as a property")
  }

  def propertyToAttribute(prop: Any): AttributeValue = prop match {
    case b: Boolean => AttributeBoolean(b)
    case n: java.math.BigDecimal => AttributeNumber(n)
    case s: String => AttributeString(s)
    case l: java.util.List[_] => AttributeValueList(l.map(propertyToAttribute(_)))
    case _ => throw new IllegalArgumentException("Cannot deserialize " + prop + " as an attribute")
  }
}

object WorkspaceJsonSupport extends JsonSupport {

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat5(Entity)

  implicit val WorkspaceFormat = jsonFormat5(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodConfigurationFormat = jsonFormat10(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat7(MethodConfigurationShort)

  implicit val MethodRepoConfigurationQueryFormat = jsonFormat4(MethodRepoConfigurationQuery)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)
}
