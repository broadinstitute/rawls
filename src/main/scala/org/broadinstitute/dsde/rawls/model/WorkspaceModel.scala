package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel.WorkspaceAccessLevel
import org.joda.time.DateTime

object Attributable {
  val reservedAttributeNames = Set("name", "entityType")
}

trait Attributable {
  def attributes: Map[String, Attribute]
  def briefName: String
}

trait DomainObject {
  //the name of a field on this object that uniquely identifies it relative to any graph siblings
  def idField: String
}

/**
 * Created by dvoet on 4/24/15.
 */
case class WorkspaceName(
                      namespace: String,
                      name: String) {
  override def toString = namespace + "/" + name // used in error messages
  def path = s"/workspaces/${namespace}/${name}"
}

case class WorkspaceRequest (
                      namespace: String,
                      name: String,
                      attributes: Map[String, Attribute]
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
}

case class Workspace (
                      namespace: String,
                      name: String,
                      workspaceId: String,
                      bucketName: String,
                      createdDate: DateTime,
                      createdBy: String,
                      attributes: Map[String, Attribute],
                      isLocked: Boolean = false
                      ) extends Attributable with DomainObject {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
  def idField = "name"
}

case class WorkspaceSubmissionStats(lastSuccessDate: Option[DateTime],
                                    lastFailureDate: Option[DateTime],
                                    runningSubmissionsCount: Int)


case class EntityName(
                   name: String)

case class Entity(
                   name: String,
                   entityType: String,
                   attributes: Map[String, Attribute]
                   ) extends Attributable with DomainObject {
  def briefName = name
  def path( workspaceName: WorkspaceName ) = s"${workspaceName.path}/entities/${name}"
  def idField = "name"
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
                   methodVersion: Int
                   ) extends DomainObject {
  def idField = "methodName"
}

case class MethodConfiguration(
                   namespace: String,
                   name: String,
                   rootEntityType: String,
                   prerequisites: Map[String, AttributeString],
                   inputs: Map[String, AttributeString],
                   outputs: Map[String, AttributeString],
                   methodRepoMethod:MethodRepoMethod
                   ) extends DomainObject {
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodRepoMethod, namespace)
  def path( workspaceName: WorkspaceName ) = workspaceName.path+s"/methodConfigs/${namespace}/${name}"
  def idField = "name"
}

case class MethodConfigurationShort(
                                name: String,
                                rootEntityType: String,
                                methodRepoMethod:MethodRepoMethod,
                                namespace: String) extends DomainObject {
  def idField = "name"
}

case class ValidatedMethodConfiguration(
                                         methodConfiguration: MethodConfiguration,
                                         validInputs: Seq[String],
                                         invalidInputs: Map[String,String],
                                         validOutputs: Seq[String],
                                         invalidOutputs: Map[String,String])

case class MethodRepoConfigurationImport(
                                         methodRepoNamespace: String,
                                         methodRepoName: String,
                                         methodRepoSnapshotId: Int,
                                         destination: MethodConfigurationName
                                         )

case class MethodRepoConfigurationExport(
                                         methodRepoNamespace: String,
                                         methodRepoName: String,
                                         source: MethodConfigurationName
                                         )

case class ConflictingEntities(conflicts: Seq[String])

case class WorkspaceListResponse(accessLevel: WorkspaceAccessLevel,
                                 workspace: Workspace,
                                 workspaceSubmissionStats: WorkspaceSubmissionStats,
                                 owners: Seq[String])

case class WorkspacePermissionsPair(workspaceId: String,
                                    accessLevel: WorkspaceAccessLevel)

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

  implicit val EntityFormat = jsonFormat3(Entity)

  implicit val WorkspaceRequestFormat = jsonFormat3(WorkspaceRequest)

  implicit val WorkspaceFormat = jsonFormat8(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodStoreMethodFormat = jsonFormat3(MethodRepoMethod)

  implicit val MethodConfigurationFormat = jsonFormat7(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat4(MethodConfigurationShort)

  implicit val MethodRepoConfigurationImportFormat = jsonFormat4(MethodRepoConfigurationImport)

  implicit val MethodRepoConfigurationExportFormat = jsonFormat3(MethodRepoConfigurationExport)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)

  implicit val WorkspaceSubmissionStatsFormat = jsonFormat3(WorkspaceSubmissionStats)

  implicit val WorkspaceAccessLevelFormat = WorkspaceACLJsonSupport.WorkspaceAccessLevelFormat

  implicit val WorkspaceListResponseFormat = jsonFormat4(WorkspaceListResponse)

  implicit val ValidatedMethodConfigurationFormat = jsonFormat5(ValidatedMethodConfiguration)

}
