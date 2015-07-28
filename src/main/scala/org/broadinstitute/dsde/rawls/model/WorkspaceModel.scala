package org.broadinstitute.dsde.rawls.model

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
case class WorkspaceName(
                      namespace: String,
                      name: String) extends Identifiable {
  def path : String = "workspaces/" + namespace + "/" + name
}

case class WorkspaceRequest (
                      @(VertexProperty@field)
                      namespace: String,
                      @(VertexProperty@field)
                      name: String,
                      attributes: Map[String, Attribute]
                      ) extends Identifiable with Attributable {
  def path = WorkspaceName(namespace,name).path
}

case class Workspace (
                      @(VertexProperty@field)
                      namespace: String,
                      @(VertexProperty@field)
                      name: String,
                      @(VertexProperty@field)
                      bucketName: String,
                      @(VertexProperty@field)
                      createdDate: DateTime,
                      @(VertexProperty@field)
                      createdBy: String,
                      attributes: Map[String, Attribute]
                      ) extends Identifiable with Attributable {
  def path = toWorkspaceName.path
  def toWorkspaceName = WorkspaceName(namespace,name)
}

case class EntityName(
                   name: String)

case class Entity(
                   @(VertexProperty@field)
                   name: String,
                   @(VertexProperty@field)
                   entityType: String,
                   attributes: Map[String, Attribute],
                   workspaceName:WorkspaceName,
                   @(VertexProperty@field)
                   vaultId:String="") extends Identifiable with Attributable {
  def path : String = workspaceName.path + "/entities/" + name
}

case class MethodConfigurationName(
                   name: String,
                   namespace: String,
                   workspaceName: WorkspaceName
                   )

case class MethodConfigurationNamePair(
                   source: MethodConfigurationName,
                   destination: MethodConfigurationName
                   )

case class EntityCopyDefinition(
                   sourceWorkspace: WorkspaceName,
                   destinationWorkspace: WorkspaceName,
                   entityType: String,
                   entityNames: Seq[String]
                   )

case class MethodStoreMethod(
                   @(VertexProperty@field)
                   methodNamespace: String,
                   @(VertexProperty@field)
                   methodName: String,
                   @(VertexProperty@field)
                   methodVersion: String
                   )
case class MethodStoreConfiguration(
                   @(VertexProperty@field)
                   methodConfigNamespace: String,
                   @(VertexProperty@field)
                   methodConfigName: String,
                   @(VertexProperty@field)
                   methodConfigVersion: String
                   )

case class MethodConfiguration(
                   @(VertexProperty@field)
                   namespace: String,
                   @(VertexProperty@field)
                   name: String,
                   @(VertexProperty@field)
                   rootEntityType: String,
                   prerequisites: Map[String, String],
                   inputs: Map[String, String],
                   outputs: Map[String, String],
                   workspaceName:WorkspaceName,
                   methodStoreConfig:MethodStoreConfiguration,
                   methodStoreMethod:MethodStoreMethod
                   ) extends Identifiable {
  def path : String = workspaceName.path + "/methodConfigs/" + namespace + "/" + name
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodStoreConfig, methodStoreMethod, workspaceName, namespace)
}
case class MethodConfigurationShort(
                                @(VertexProperty@field)
                                name: String,
                                @(VertexProperty@field)
                                rootEntityType: String,
                                methodStoreConfig:MethodStoreConfiguration,
                                methodStoreMethod:MethodStoreMethod,
                                @(VertexProperty@field)
                                workspaceName:WorkspaceName,
                                @(VertexProperty@field)
                                namespace: String)

case class MethodRepoConfigurationQuery(
                                         methodRepoNamespace: String,
                                         methodRepoName: String,
                                         methodRepoSnapshotId: String,
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

  implicit val WorkspaceRequestFormat = jsonFormat3(WorkspaceRequest)

  implicit val WorkspaceFormat = jsonFormat6(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodStoreMethodFormat = jsonFormat3(MethodStoreMethod)

  implicit val MethodStoreConfigurationFormat = jsonFormat3(MethodStoreConfiguration)

  implicit val MethodConfigurationFormat = jsonFormat9(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat6(MethodConfigurationShort)

  implicit val MethodRepoConfigurationQueryFormat = jsonFormat4(MethodRepoConfigurationQuery)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)
}
