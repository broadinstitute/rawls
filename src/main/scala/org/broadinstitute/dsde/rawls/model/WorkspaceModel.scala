package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime

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
  override def toString = namespace + "/" + name // used in error messages
}

case class WorkspaceRequest (
                      namespace: String,
                      name: String,
                      attributes: Map[String, Attribute]
                      ) extends Identifiable with Attributable {
  def path = WorkspaceName(namespace,name).path
  def toWorkspaceName = WorkspaceName(namespace,name)
}

case class Workspace (
                      namespace: String,
                      name: String,
                      bucketName: String,
                      createdDate: DateTime,
                      createdBy: String,
                      attributes: Map[String, Attribute]
                      ) extends Identifiable with Attributable {
  def path = toWorkspaceName.path
  def toWorkspaceName = WorkspaceName(namespace,name)
}

case class EntityName(
                   name: String)

case class Entity(
                   name: String,
                   entityType: String,
                   attributes: Map[String, Attribute],
                   workspaceName:WorkspaceName) extends Identifiable with Attributable {
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

case class MethodRepoMethod(
                   methodNamespace: String,
                   methodName: String,
                   methodVersion: String
                   )

case class MethodRepoConfiguration(
                   methodConfigNamespace: String,
                   methodConfigName: String,
                   methodConfigVersion: String
                   )

case class MethodConfiguration(
                   namespace: String,
                   name: String,
                   rootEntityType: String,
                   prerequisites: Map[String, AttributeString],
                   inputs: Map[String, AttributeString],
                   outputs: Map[String, AttributeString],
                   workspaceName:WorkspaceName,
                   methodRepoConfig:MethodRepoConfiguration,
                   methodRepoMethod:MethodRepoMethod
                   ) extends Identifiable {
  def path : String = workspaceName.path + "/methodConfigs/" + namespace + "/" + name
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodRepoConfig, methodRepoMethod, workspaceName, namespace)
}
case class MethodConfigurationShort(
                                name: String,
                                rootEntityType: String,
                                methodStoreConfig:MethodRepoConfiguration,
                                methodStoreMethod:MethodRepoMethod,
                                workspaceName:WorkspaceName,
                                namespace: String)

case class MethodRepoConfigurationQuery(
                                         methodRepoNamespace: String,
                                         methodRepoName: String,
                                         methodRepoSnapshotId: String,
                                         destination: MethodConfigurationName
                                         )

case class ConflictingEntities(conflicts: Seq[String])

sealed trait Attribute
sealed trait AttributeValue extends Attribute

case object AttributeNull extends AttributeValue
case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case object AttributeEmptyList extends Attribute
case class AttributeValueList(val list: Seq[AttributeValue]) extends Attribute
case class AttributeEntityReferenceList(val list: Seq[AttributeEntityReference]) extends Attribute
case class AttributeEntityReference(val entityType: String, val entityName: String) extends Attribute

object AttributeConversions {
  // need to do some casting to conform to this list: http://orientdb.com/docs/last/Types.html
  def attributeToProperty(att: AttributeValue): Any = att match {
    case AttributeBoolean(b) => b
    case AttributeNumber(n) => n.bigDecimal
    case AttributeString(s) => s
    case AttributeNull => null
    case _ => throw new IllegalArgumentException("Cannot serialize " + att + " as a property")
  }

  def propertyToAttribute(prop: Any): AttributeValue = prop match {
    case b: Boolean => AttributeBoolean(b)
    case n: java.math.BigDecimal => AttributeNumber(n)
    case s: String => AttributeString(s)
    case null => AttributeNull
    case _ => throw new IllegalArgumentException("Cannot deserialize " + prop + " as an attribute")
  }
}

object WorkspaceJsonSupport extends JsonSupport {

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat4(Entity)

  implicit val WorkspaceRequestFormat = jsonFormat3(WorkspaceRequest)

  implicit val WorkspaceFormat = jsonFormat6(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodStoreMethodFormat = jsonFormat3(MethodRepoMethod)

  implicit val MethodStoreConfigurationFormat = jsonFormat3(MethodRepoConfiguration)

  implicit val MethodConfigurationFormat = jsonFormat9(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat6(MethodConfigurationShort)

  implicit val MethodRepoConfigurationQueryFormat = jsonFormat4(MethodRepoConfigurationQuery)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)
}
