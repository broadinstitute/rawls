package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.SortDirections.SortDirection
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.joda.time.DateTime
import spray.http.StatusCode
import spray.json._

object Attributable {
  // if updating these, also update their use in SlickExpressionParsing
  val entityIdAttributeSuffix = "_id"
  val workspaceEntityType = "workspace"
  val workspaceIdAttribute = workspaceEntityType + entityIdAttributeSuffix
  val nameReservedAttribute = "name"
  val entityTypeReservedAttribute = "entityType"
  val reservedAttributeNames = Set(nameReservedAttribute, entityTypeReservedAttribute, workspaceIdAttribute)
  type AttributeMap = Map[AttributeName, Attribute]
}

trait Attributable {
  def attributes: AttributeMap
  def briefName: String
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

case class AttributeName(
                          namespace: String,
                          name: String) extends Ordered[AttributeName] {
  // enable implicit ordering for sorting
  import scala.math.Ordered.orderingToOrdered
  def compare(that: AttributeName): Int = (this.namespace, this.name) compare (that.namespace, that.name)
  def equalsIgnoreCase(that: AttributeName): Boolean = (this.namespace.equalsIgnoreCase(that.namespace) && this.name.equalsIgnoreCase(that.name))
}

object AttributeName {
  val defaultNamespace = "default"
  val libraryNamespace = "library"
  val validNamespaces = Set(AttributeName.defaultNamespace, AttributeName.libraryNamespace)

  val delimiter = ':'

  def withDefaultNS(name: String) = AttributeName(defaultNamespace, name)

  def withLibraryNS(name: String) = AttributeName(libraryNamespace, name)

  def toDelimitedName(aName: AttributeName): String = {
    if (aName.namespace == defaultNamespace) aName.name
    else aName.namespace + delimiter + aName.name
  }

  def fromDelimitedName(dName: String): AttributeName = {
    dName.split(delimiter).toList match {
      case sName :: Nil => AttributeName.withDefaultNS(sName)
      case sNamespace :: sName :: Nil => AttributeName(sNamespace, sName)
      case _ => throw new RawlsException(s"Attribute string $dName has too many '$delimiter' delimiters")
    }
  }
}

case class WorkspaceRequest (
                      namespace: String,
                      name: String,
                      realm: Option[RawlsGroupRef],
                      attributes: AttributeMap
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
}

case class Workspace(
                      namespace: String,
                      name: String,
                      realm: Option[RawlsGroupRef],
                      workspaceId: String,
                      bucketName: String,
                      createdDate: DateTime,
                      lastModified: DateTime,
                      createdBy: String,
                      attributes: AttributeMap,
                      accessLevels: Map[WorkspaceAccessLevel, RawlsGroupRef],
                      realmACLs: Map[WorkspaceAccessLevel, RawlsGroupRef],
                      isLocked: Boolean = false
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
}

case class WorkspaceSubmissionStats(lastSuccessDate: Option[DateTime],
                                    lastFailureDate: Option[DateTime],
                                    runningSubmissionsCount: Int)


case class EntityName(
                   name: String)

case class Entity(
                   name: String,
                   entityType: String,
                   attributes: AttributeMap
                   ) extends Attributable {
  def briefName = name
  def path( workspaceName: WorkspaceName ) = s"${workspaceName.path}/entities/${name}"
  def toReference = AttributeEntityReference(entityType, name)
}

case class EntityTypeMetadata(
                             count: Int,
                             idName: String,
                             attributeNames: Seq[String]
                             )

object SortDirections {
  sealed trait SortDirection
  case object Ascending extends SortDirection
  case object Descending extends SortDirection

  def fromString(dir: String) = {
    dir.toLowerCase match {
      case "asc" => Ascending
      case "desc" => Descending
      case _ => throw new RawlsException(s"$dir is not a valid sort direction")
    }
  }

  def toString(direction: SortDirection) = {
    direction match {
      case Ascending => "asc"
      case Descending => "desc"
    }
  }

  def toSql(direction: SortDirection) = toString(direction)
}
case class EntityQuery(page: Int, pageSize: Int, sortField: String, sortDirection: SortDirections.SortDirection, filterTerms: Option[String])

case class EntityQueryResultMetadata(unfilteredCount: Int, filteredCount: Int, filteredPageCount: Int)

case class EntityQueryResponse(parameters: EntityQuery, resultMetadata: EntityQueryResultMetadata, results: Seq[Entity])

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
                   )

case class MethodInput(name: String, inputType: String, optional: Boolean)
case class MethodOutput(name: String, outputType: String)
case class MethodInputsOutputs(inputs: Seq[MethodInput], outputs: Seq[MethodOutput])

case class MethodConfiguration(
                   namespace: String,
                   name: String,
                   rootEntityType: String,
                   prerequisites: Map[String, AttributeString],
                   inputs: Map[String, AttributeString],
                   outputs: Map[String, AttributeString],
                   methodRepoMethod:MethodRepoMethod,
                   deleted: Boolean = false
                   ) {
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodRepoMethod, namespace)
  def path( workspaceName: WorkspaceName ) = workspaceName.path+s"/methodConfigs/${namespace}/${name}"
}

case class MethodConfigurationShort(
                                name: String,
                                rootEntityType: String,
                                methodRepoMethod:MethodRepoMethod,
                                namespace: String)


case class AgoraMethodConfiguration(namespace: String,
                                    name: String,
                                    rootEntityType: String,
                                    prerequisites: Map[String, AttributeString],
                                    inputs: Map[String, AttributeString],
                                    outputs: Map[String, AttributeString],
                                    methodRepoMethod: MethodRepoMethod)

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

case class WorkspaceStatus(workspaceName: WorkspaceName, statuses: Map[String, String])

case class BucketUsageResponse(usageInBytes: BigInt)

case class ErrorReport(source: String, message: String, statusCode: Option[StatusCode], causes: Seq[ErrorReport], stackTrace: Seq[StackTraceElement])

object ErrorReport extends ((String,String,Option[StatusCode],Seq[ErrorReport],Seq[StackTraceElement]) => ErrorReport) {
  val SOURCE = "rawls"

  def apply(statusCode: StatusCode, message: String): ErrorReport =
    new ErrorReport(SOURCE,message,Option(statusCode),Seq.empty,Seq.empty)

  def apply(statusCode: StatusCode, message: String, cause: ErrorReport): ErrorReport =
    new ErrorReport(SOURCE,message,Option(statusCode),Seq(cause),Seq.empty)

  def apply(statusCode: StatusCode, message: String, causes: Seq[ErrorReport]): ErrorReport =
    new ErrorReport(SOURCE,message,Option(statusCode),causes,Seq.empty)

  def apply(throwable: Throwable): ErrorReport =
    new ErrorReport(SOURCE,message(throwable),None,causes(throwable),throwable.getStackTrace)

  def apply(throwable: Throwable, statusCode: StatusCode): ErrorReport =
    new ErrorReport(SOURCE,message(throwable),Some(statusCode),causes(throwable),throwable.getStackTrace)

  def apply(message: String, cause: ErrorReport) =
    new ErrorReport(SOURCE,message,None,Seq(cause),Seq.empty)

  def apply(message: String, causes: Seq[ErrorReport]) =
    new ErrorReport(SOURCE,message,None,causes,Seq.empty)

  def message(throwable: Throwable) = Option(throwable.getMessage).getOrElse(throwable.getClass.getSimpleName)
  def causes(throwable: Throwable): Array[ErrorReport] = causeThrowables(throwable).map(ErrorReport(_))
  private def causeThrowables(throwable: Throwable) = {
    if (throwable.getSuppressed.nonEmpty || throwable.getCause == null) throwable.getSuppressed
    else Array(throwable.getCause)
  }
}

case class ApplicationVersion(gitHash: String, buildNumber: String, version: String)

sealed trait Attribute
sealed trait AttributeListElementable extends Attribute //terrible name for "this type can legally go in an attribute list"
sealed trait AttributeValue extends AttributeListElementable
sealed trait AttributeList[T <: AttributeListElementable] extends Attribute { val list: Seq[T] }
case object AttributeNull extends AttributeValue
case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case object AttributeValueEmptyList extends AttributeList[AttributeValue] { val list = Seq.empty }
case object AttributeEntityReferenceEmptyList extends AttributeList[AttributeEntityReference] { val list = Seq.empty }
case class AttributeValueList(val list: Seq[AttributeValue]) extends AttributeList[AttributeValue]
case class AttributeEntityReferenceList(val list: Seq[AttributeEntityReference]) extends AttributeList[AttributeEntityReference]
case class AttributeEntityReference(val entityType: String, val entityName: String) extends AttributeListElementable

object AttributeStringifier {
  def apply(attribute: Attribute): String = {
    attribute match {
      case AttributeNull => ""
      case AttributeString(value) => value
      case AttributeNumber(value) => value.toString()
      case AttributeBoolean(value) => value.toString()
      case AttributeEntityReference(t, name) => name
      case al: AttributeList[_] => al.list.map(apply).mkString(" ")
    }
  }
}

object WorkspaceJsonSupport extends JsonSupport {

  implicit object SortDirectionFormat extends JsonFormat[SortDirection] {
    override def write(dir: SortDirection): JsValue = JsString(SortDirections.toString(dir))

    override def read(json: JsValue): SortDirection = json match {
      case JsString(dir) => SortDirections.fromString(dir)
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object AttributeNameFormat extends JsonFormat[AttributeName] {
    override def write(an: AttributeName): JsValue = JsString(AttributeName.toDelimitedName(an))

    override def read(json: JsValue): AttributeName = json match {
      case JsString(name) => AttributeName.fromDelimitedName(name)
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat3(Entity)

  implicit val RawlsGroupRefFormat = UserModelJsonSupport.RawlsGroupRefFormat

  implicit val WorkspaceRequestFormat = jsonFormat4(WorkspaceRequest)

  implicit val WorkspaceAccessLevelFormat = WorkspaceACLJsonSupport.WorkspaceAccessLevelFormat

  implicit val WorkspaceFormat = jsonFormat12(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val EntityTypeMetadataFormat = jsonFormat3(EntityTypeMetadata)

  implicit val EntityQueryFormat = jsonFormat5(EntityQuery)

  implicit val EntityQueryResultMetadataFormat = jsonFormat3(EntityQueryResultMetadata)

  implicit val EntityQueryResponseFormat = jsonFormat3(EntityQueryResponse)

  implicit val WorkspaceStatusFormat = jsonFormat2(WorkspaceStatus)

  implicit val BucketUsageResponseFormat = jsonFormat1(BucketUsageResponse)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodStoreMethodFormat = jsonFormat3(MethodRepoMethod)

  implicit val MethodConfigurationFormat = jsonFormat8(MethodConfiguration)

  implicit val AgoraMethodConfigurationFormat = jsonFormat7(AgoraMethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat4(MethodConfigurationShort)

  implicit val MethodRepoConfigurationImportFormat = jsonFormat4(MethodRepoConfigurationImport)

  implicit val MethodRepoConfigurationExportFormat = jsonFormat3(MethodRepoConfigurationExport)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)

  implicit val WorkspaceSubmissionStatsFormat = jsonFormat3(WorkspaceSubmissionStats)

  implicit val WorkspaceListResponseFormat = jsonFormat4(WorkspaceListResponse)

  implicit val ValidatedMethodConfigurationFormat = jsonFormat5(ValidatedMethodConfiguration)

  implicit val MethodInputFormat = jsonFormat3(MethodInput)

  implicit val MethodOutputFormat = jsonFormat2(MethodOutput)

  implicit val MethodInputsOutputsFormat = jsonFormat2(MethodInputsOutputs)

  implicit object StatusCodeFormat extends JsonFormat[StatusCode] {
    override def write(code: StatusCode): JsValue = JsNumber(code.intValue)

    override def read(json: JsValue): StatusCode = json match {
      case JsNumber(n) => n.intValue
      case _ => throw new DeserializationException("unexpected json type")
    }
  }

  implicit object StackTraceElementFormat extends RootJsonFormat[StackTraceElement] {
    val CLASS_NAME = "className"
    val METHOD_NAME = "methodName"
    val FILE_NAME = "fileName"
    val LINE_NUMBER = "lineNumber"

    def write(stackTraceElement: StackTraceElement) =
      JsObject( CLASS_NAME -> JsString(stackTraceElement.getClassName),
                METHOD_NAME -> JsString(stackTraceElement.getMethodName),
                FILE_NAME -> JsString(stackTraceElement.getFileName),
                LINE_NUMBER -> JsNumber(stackTraceElement.getLineNumber) )

    def read(json: JsValue) =
      json.asJsObject.getFields(CLASS_NAME,METHOD_NAME,FILE_NAME,LINE_NUMBER) match {
        case Seq(JsString(className), JsString(methodName), JsString(fileName), JsNumber(lineNumber)) =>
          new StackTraceElement(className,methodName,fileName,lineNumber.toInt)
        case _ => throw new DeserializationException("unable to deserialize StackTraceElement")
      }
  }

  implicit val ErrorReportFormat: RootJsonFormat[ErrorReport] = rootFormat(lazyFormat(jsonFormat(ErrorReport,"source","message","statusCode","causes","stackTrace")))

  implicit val ApplicationVersionFormat = jsonFormat3(ApplicationVersion)
}
