package org.broadinstitute.dsde.rawls.model

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.SortDirections.SortDirection
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.joda.time.DateTime
import com.netaporter.uri.Uri.parse
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.BadRequest
import spray.json._
import UserModelJsonSupport.ManagedGroupRefFormat

import scala.util.Try

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
  val tagsNamespace = "tag"
  // removed library from the set because these attributes should no longer be set with updateWorkspace
  val validNamespaces = Set(AttributeName.defaultNamespace, AttributeName.tagsNamespace)

  val delimiter = ':'

  def withDefaultNS(name: String) = AttributeName(defaultNamespace, name)

  def withLibraryNS(name: String) = AttributeName(libraryNamespace, name)

  def withTagsNS() = AttributeName(tagsNamespace, "tags")

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
                              attributes: AttributeMap,
                              authorizationDomain: Option[Set[ManagedGroupRef]] = Option(Set.empty),
                              copyFilesWithPrefix: Option[String] = None
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName: String = toWorkspaceName.toString
  def path: String = toWorkspaceName.path
}

case class Workspace(
                      namespace: String,
                      name: String,
                      workspaceId: String,
                      bucketName: String,
                      workflowCollectionName: Option[String],
                      createdDate: DateTime,
                      lastModified: DateTime,
                      createdBy: String,
                      attributes: AttributeMap,
                      isLocked: Boolean = false
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName: String = toWorkspaceName.toString
  def path: String = toWorkspaceName.path
  lazy val workspaceIdAsUUID: UUID = UUID.fromString(workspaceId)
}

case class WorkspaceSubmissionStats(lastSuccessDate: Option[DateTime],
                                    lastFailureDate: Option[DateTime],
                                    runningSubmissionsCount: Int)

case class WorkspaceBucketOptions(requesterPays: Boolean)

case class EntityName(
                   name: String)

case class Entity(
                   name: String,
                   entityType: String,
                   attributes: AttributeMap
                   ) extends Attributable {
  def briefName: String = name
  def path( workspaceName: WorkspaceName ) = s"${workspaceName.path}/entities/${entityType}/${name}"
  def path( workspace: Workspace ): String = path(workspace.toWorkspaceName)
  def path( workspaceRequest: WorkspaceRequest ): String = path(workspaceRequest.toWorkspaceName)
  def toReference = AttributeEntityReference(entityType, name)
}

case class EntityTypeMetadata(
                             count: Int,
                             idName: String,
                             attributeNames: Seq[String]
                             )

object EntityDeleteRequest {
  def apply(entities: Entity*): Seq[AttributeEntityReference] = entities map { _.toReference }
}

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

case class EntityCopyResponse(entitiesCopied: Seq[AttributeEntityReference], hardConflicts: Seq[EntityHardConflict], softConflicts: Seq[EntitySoftConflict])

case class EntitySoftConflict(entityType: String, entityName: String, conflicts: Seq[EntitySoftConflict])

case class EntityHardConflict(entityType: String, entityName: String)

case class EntityPath(path: Seq[AttributeEntityReference])

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

object ImportStatuses {
  sealed trait ImportStatus extends RawlsEnumeration[ImportStatus] {
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String): ImportStatus = ImportStatuses.withName(name)
  }

  def withName(name: String): ImportStatus = name.toLowerCase match {
    case "readyforupsert" => ReadyForUpsert
    case "upserting" => Upserting
    case "done" => Done
    case "error" => Error
    case _ => throw new RawlsException(s"invalid ImportStatus [${name}]")
  }

  case object ReadyForUpsert extends ImportStatus
  case object Upserting extends ImportStatus
  case object Done extends ImportStatus
  case object Error extends ImportStatus
}



sealed trait MethodRepoMethod {

  def methodUri: String

  def repo: MethodRepository

  def validate: Option[MethodRepoMethod]

}

object MethodRepoMethod {

  def fromUri(uri: String): MethodRepoMethod = {
    (for {
      parsedUri <- Try(parse(uri)).toOption
      repoName  <- parsedUri.scheme
      repo      <- MethodRepository.withName(repoName)
    } yield {
      repo
    }) match {
      case Some(Agora) => AgoraMethod(uri)
      case Some(Dockstore) => DockstoreMethod(uri)
      case _ => throw new RawlsException(s"Illegal method repo specified in URI \'$uri\'")
    }
  }

  def apply(methodNamespace: String, methodName: String, methodVersion: Int): AgoraMethod = AgoraMethod(methodNamespace, methodName, methodVersion)

  def apply(methodPath: String, methodVersion: String): DockstoreMethod = DockstoreMethod(methodPath, methodVersion)

}

case class AgoraMethod(methodNamespace: String, methodName: String, methodVersion: Int) extends MethodRepoMethod {

  override def validate: Option[AgoraMethod] = {
    if (methodNamespace.nonEmpty && methodName.nonEmpty && methodVersion > 0)
      Some(this)
    else
      None
  }

  override def methodUri: String = {
    if (validate.isDefined)
      s"${repo.scheme}://${URLEncoder.encode(methodNamespace, UTF_8.name)}/${URLEncoder.encode(methodName, UTF_8.name)}/$methodVersion"
    else
      throw new RawlsException(
        s"Could not generate a method URI from AgoraMethod with namespace \'$methodNamespace\', name \'$methodName\', version \'$methodVersion\'"
      )
  }

  override def repo: MethodRepository = Agora
}

object AgoraMethod {

  def apply(uri: String): AgoraMethod = {

    (for {
      parsedUri <- Try(parse(uri)).toOption
      namespace <- parsedUri.host // parser does not URL-decode host
      parts     <- Option(parsedUri.pathParts)
      name      <- Option(parts.head.part) // parser does URL-decode path parts
      version   <- Try(parts(1).part.toInt).toOption // encoding does not apply to ints
      result    <- if (parts.size == 2) AgoraMethod(URLDecoder.decode(namespace, UTF_8.name), name, version).validate else None
    } yield {
      result
    }).getOrElse(throw new RawlsException(s"Could not create an AgoraMethod from URI \'$uri\'"))
  }

}

case class DockstoreMethod(methodPath: String, methodVersion: String) extends MethodRepoMethod {

  override def validate: Option[DockstoreMethod] = {
    if (methodPath.nonEmpty && methodVersion.nonEmpty)
      Some(this)
    else
      None
  }

  override def methodUri: String = {
    if (validate.isDefined)
      s"${repo.scheme}://${URLEncoder.encode(methodPath, UTF_8.name)}/${URLEncoder.encode(methodVersion, UTF_8.name)}"
    else
      throw new RawlsException(
        s"Could not generate a method URI from DockstoreMethod with path \'$methodPath\', version \'$methodVersion\'"
      )
  }

  override def repo: MethodRepository = Dockstore

  private def methodVersionEncoded: String = URLEncoder.encode(methodVersion, UTF_8.name)

  private def toolId = s"#workflow/$methodPath"

  private def toolIdEncoded = URLEncoder.encode(toolId, UTF_8.name)

  def ga4ghDescriptorUrl(baseUrl: String): String =
    s"$baseUrl/ga4gh/v1/tools/$toolIdEncoded/versions/$methodVersionEncoded/WDL/descriptor"
}

object DockstoreMethod {

  def apply(uri: String): DockstoreMethod = {

    (for {
      parsedUri <- Try(parse(uri)).toOption
      path      <- parsedUri.host // parser does not URL-decode host
      parts     <- Option(parsedUri.pathParts)
      version   <- Try(parts.head.part).toOption // parser does URL-decode path parts
      result    <- if (parts.size == 1) DockstoreMethod(URLDecoder.decode(path, UTF_8.name), version).validate else None
    } yield {
      result
    }).getOrElse(throw new RawlsException(s"Could not create a DockstoreMethod from URI \'$uri\'"))
  }

}

sealed trait MethodRepository {
  val scheme: String
}

case object Agora extends MethodRepository {
  override val scheme: String = "agora"
}

case object Dockstore extends MethodRepository {
  override val scheme: String = "dockstore"
}

object MethodRepository {

  def withName(name: String): Option[MethodRepository] = name.toLowerCase match {
    case Agora.scheme => Some(Agora)
    case Dockstore.scheme => Some(Dockstore)
    case _ => None
  }

  val all: Set[MethodRepository] = Set(Agora, Dockstore)
}

case class GA4GHTool(`type`: String, descriptor: String, url: String)
case class MethodInput(name: String, inputType: String, optional: Boolean)
case class MethodOutput(name: String, outputType: String)
case class MethodInputsOutputs(inputs: Seq[MethodInput], outputs: Seq[MethodOutput])

case class MethodConfiguration(
                   namespace: String,
                   name: String,
                   rootEntityType: Option[String],
                   //we used to have prereqs but did nothing with them. so we removed them.
                   //leaving it as an option means we can accept it being there or not; when we return this object,
                   //we'll always put Some(Map.empty) here so that clients who might be expecting this key still get it.
                   prerequisites: Option[Map[String, AttributeString]],
                   inputs: Map[String, AttributeString],
                   outputs: Map[String, AttributeString],
                   methodRepoMethod: MethodRepoMethod,
                   methodConfigVersion: Int = 1,
                   deleted: Boolean = false,
                   deletedDate: Option[DateTime] = None,
                   dataReferenceName: Option[DataReferenceName] = None
                   ) {
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodRepoMethod, namespace)
  def path( workspaceName: WorkspaceName ): String = workspaceName.path+s"/methodconfigs/${namespace}/${name}"
  def path( workspace: Workspace ): String = path(workspace.toWorkspaceName)
  def toId: String = s"$namespace/$name/$methodConfigVersion"
}

case class MethodConfigurationShort(
                                name: String,
                                rootEntityType: Option[String],
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
                                         validInputs: Set[String],
                                         invalidInputs: Map[String,String],
                                         missingInputs: Set[String],
                                         extraInputs: Set[String],
                                         validOutputs: Set[String],
                                         invalidOutputs: Map[String,String])

case class ValidatedMCExpressions(
                                         validInputs: Set[String],
                                         invalidInputs: Map[String,String],
                                         validOutputs: Set[String],
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

case class WorkspaceListResponse(accessLevel: WorkspaceAccessLevel,
                                 workspace: WorkspaceDetails,
                                 workspaceSubmissionStats: Option[WorkspaceSubmissionStats],
                                 public: Boolean)

case class WorkspaceResponse(accessLevel: Option[WorkspaceAccessLevel],
                             canShare: Option[Boolean],
                             canCompute: Option[Boolean],
                             catalog: Option[Boolean],
                             workspace: WorkspaceDetails,
                             workspaceSubmissionStats: Option[WorkspaceSubmissionStats],
                             bucketOptions: Option[WorkspaceBucketOptions],
                             owners: Option[Set[String]])

case class WorkspaceDetails(namespace: String,
                            name: String,
                            workspaceId: String,
                            bucketName: String,
                            workflowCollectionName: Option[String],
                            createdDate: DateTime,
                            lastModified: DateTime,
                            createdBy: String,
                            attributes: Option[AttributeMap],
                            isLocked: Boolean = false,
                            authorizationDomain: Option[Set[ManagedGroupRef]]) {
  def toWorkspace: Workspace = Workspace(namespace, name, workspaceId, bucketName, workflowCollectionName, createdDate, lastModified, createdBy, attributes.getOrElse(Map()), isLocked)
}


case class WorkspaceFieldSpecs(fields: Option[Set[String]] = None)
object WorkspaceFieldSpecs {
  def fromQueryParams(params: Seq[(String, String)], paramName: String): WorkspaceFieldSpecs = {
    // ensure the "fields" parameter only exists once
    val paramValues:Seq[String] = params.filter(_._1.equals(paramName)).map(_._2)
    if (paramValues.size > 1) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(BadRequest, s"Parameter '$paramName' may not be present multiple times.")(ErrorReportSource("rawls")))
    } else if (paramValues.isEmpty) {
      new WorkspaceFieldSpecs(None)
    } else {
      // un-delimit the (single) param value
      // The use of a delimited list here is informed by both the JSON:API spec and Google’s FieldMask syntax;
      // it also reduces the overall length of the URL in the presence of many values.
      val splitParamValues = paramValues.head.split(',').map(_.trim)
      new WorkspaceFieldSpecs(Option(splitParamValues.toSet))
    }
  }
}

/** Criteria to use when reading one or more workspaces: which attributes should be returned?
  * if `all` is true, always return all attributes for this workspace.
  * if `all` is false, but `attrsToSelect` is populated, return only the attrs in `attrsToSelect`.
  */
case class WorkspaceAttributeSpecs(all: Boolean, attrsToSelect: List[AttributeName] = List.empty[AttributeName])


/** Contains List[String]s with the names of the members of the WorkspaceResponse
  * and WorkspaceDetails case classes. Also contains the concatenation of those two lists,
  * with the WorkspaceDetails members prefixed by "workspace." This concatenated list
  * represents the keys present in in a JSON-serialized WorkspaceResponse object.
  *
  * Since WorkspaceFieldNames uses reflection (slow!) to find these names, we build it
  * as an object so it's only calculated once.
  */
object WorkspaceFieldNames {
  import scala.reflect.runtime.universe._
  def classAccessors[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name.toString
  }.toList
  lazy val workspaceResponseClassNames: List[String] = classAccessors[WorkspaceResponse]
  lazy val workspaceListResponseClassNames: List[String] = classAccessors[WorkspaceListResponse]
  lazy val workspaceDetailClassNames: List[String] = classAccessors[WorkspaceDetails]

  lazy val workspaceResponseFieldNames: Set[String] = (workspaceResponseClassNames ++ workspaceDetailClassNames.map(k => s"workspace.$k")).toSet
  lazy val workspaceListResponseFieldNames: Set[String] = (workspaceListResponseClassNames ++ workspaceDetailClassNames.map(k => s"workspace.$k")).toSet

}

object WorkspaceDetails {
  def apply(workspace: Workspace, authorizationDomain: Set[ManagedGroupRef]): WorkspaceDetails = {
    fromWorkspaceAndOptions(workspace, Option(authorizationDomain),true)
  }
  def fromWorkspaceAndOptions(workspace: Workspace, optAuthorizationDomain: Option[Set[ManagedGroupRef]], useAttributes: Boolean): WorkspaceDetails = {
    WorkspaceDetails(
      workspace.namespace,
      workspace.name,
      workspace.workspaceId,
      workspace.bucketName,
      workspace.workflowCollectionName,
      workspace.createdDate,
      workspace.lastModified,
      workspace.createdBy,
      if (useAttributes) Option(workspace.attributes) else None,
      workspace.isLocked,
      optAuthorizationDomain
    )
  }
}

case class ManagedGroupAccessInstructions(groupName: String, instructions: String)

case class WorkspacePermissionsPair(workspaceId: String,
                                    accessLevel: WorkspaceAccessLevel)

case class WorkspaceStatus(workspaceName: WorkspaceName, statuses: Map[String, String])

case class BucketUsageResponse(usageInBytes: BigInt)

case class ErrorReport(source: String, message: String, statusCode: Option[StatusCode], causes: Seq[ErrorReport], stackTrace: Seq[StackTraceElement], exceptionClass: Option[Class[_]])

case class ErrorReportSource(source: String)

object ErrorReport {
  def apply(message: String)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,Seq.empty,Seq.empty, None)

  def apply(message: String, cause: ErrorReport)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,Seq(cause),Seq.empty, None)

  def apply(message: String, causes: Seq[ErrorReport])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,None,causes,Seq.empty, None)

  def apply(statusCode: StatusCode, throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message(throwable),Some(statusCode),causes(throwable),throwable.getStackTrace,Option(throwable.getClass))

  def apply(statusCode: StatusCode, message: String)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),Seq.empty,Seq.empty, None)

  def apply(statusCode: StatusCode, message: String, throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source, message, Option(statusCode), causes(throwable), throwable.getStackTrace, None)

  def apply(statusCode: StatusCode, message: String, cause: ErrorReport)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),Seq(cause),Seq.empty, None)

  def apply(statusCode: StatusCode, message: String, causes: Seq[ErrorReport])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message,Option(statusCode),causes,Seq.empty, None)

  def apply(throwable: Throwable)(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source,message(throwable),None,causes(throwable),throwable.getStackTrace,Option(throwable.getClass))

  def apply(message: String, statusCode: Option[StatusCode], causes: Seq[ErrorReport], stackTrace: Seq[StackTraceElement], exceptionClass: Option[Class[_]])(implicit source: ErrorReportSource): ErrorReport =
    ErrorReport(source.source, message, statusCode, causes, stackTrace, exceptionClass)

  def message(throwable: Throwable): String = Option(throwable.getMessage).getOrElse(throwable.getClass.getSimpleName)

  def causes(throwable: Throwable)(implicit source: ErrorReportSource): Array[ErrorReport] = causeThrowables(throwable).map(apply)

  private def causeThrowables(throwable: Throwable) = {
    if (throwable.getSuppressed.nonEmpty || throwable.getCause == null) throwable.getSuppressed
    else Array(throwable.getCause)
  }
}

case class ApplicationVersion(gitHash: String, buildNumber: String, version: String)

case object AttributeValueRawJson {
  def apply(str: String) : AttributeValueRawJson = AttributeValueRawJson(str.parseJson)
}

sealed trait Attribute
sealed trait AttributeListElementable extends Attribute //terrible name for "this type can legally go in an attribute list"
sealed trait AttributeValue extends AttributeListElementable
sealed trait AttributeList[T <: AttributeListElementable] extends Attribute { val list: Seq[T] }
case object AttributeNull extends AttributeValue
case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case class AttributeValueRawJson(val value: JsValue) extends AttributeValue
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
      case AttributeValueRawJson(value) => value.toString()
      case AttributeEntityReference(t, name) => name
      case al: AttributeList[_] =>
        WDLJsonSupport.attributeFormat.write(al).toString()
    }
  }
}

case class WorkspaceTag(tag: String, count: Int)

class WorkspaceJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._
  import WorkspaceACLJsonSupport.WorkspaceAccessLevelFormat
  import DataReferenceModelJsonSupport.DataReferenceNameFormat

  implicit object SortDirectionFormat extends JsonFormat[SortDirection] {
    override def write(dir: SortDirection): JsValue = JsString(SortDirections.toString(dir))

    override def read(json: JsValue): SortDirection = json match {
      case JsString(dir) => SortDirections.fromString(dir)
      case _ => throw DeserializationException("unexpected json type")
    }
  }

  implicit object AttributeNameFormat extends JsonFormat[AttributeName] {
    override def write(an: AttributeName): JsValue = JsString(AttributeName.toDelimitedName(an))

    override def read(json: JsValue): AttributeName = json match {
      case JsString(name) => AttributeName.fromDelimitedName(name)
      case _ => throw DeserializationException("unexpected json type")
    }
  }

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat3(Entity)

  implicit val WorkspaceRequestFormat = jsonFormat5(WorkspaceRequest)

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

  implicit val EntitySoftConflictFormat: JsonFormat[EntitySoftConflict] = lazyFormat(jsonFormat3(EntitySoftConflict))

  implicit val EntityHardConflictFormat = jsonFormat2(EntityHardConflict)

  implicit val EntityCopyResponseFormat = jsonFormat3(EntityCopyResponse)

  implicit val AgoraMethodFormat = jsonFormat3(AgoraMethod.apply)

  implicit val DockstoreMethodFormat = jsonFormat2(DockstoreMethod.apply)

  implicit object MethodRepoMethodFormat extends RootJsonFormat[MethodRepoMethod] {

    override def write(method: MethodRepoMethod): JsValue = {
      method match {
        case agora: AgoraMethod =>
          JsObject(Map("methodUri" -> JsString(agora.methodUri), "sourceRepo" -> JsString(agora.repo.scheme)) ++ agora.toJson.asJsObject.fields)
        case dockstore: DockstoreMethod =>
          JsObject(Map("methodUri" -> JsString(dockstore.methodUri), "sourceRepo" -> JsString(dockstore.repo.scheme)) ++ dockstore.toJson.asJsObject.fields)
      }
    }

    override def read(json: JsValue): MethodRepoMethod = {
      val fromUri = json.asJsObject.fields.get("methodUri") match {
        case Some(JsString(uri)) => Try(MethodRepoMethod.fromUri(uri)).toOption
        case _ => None
      }

      fromUri match {
        case Some(method) => method
        case _ =>
          json.asJsObject.fields.get("sourceRepo") match {
            case Some(JsString(Dockstore.scheme)) => DockstoreMethodFormat.read(json)
            case Some(JsString(Agora.scheme)) => AgoraMethodFormat.read(json)
            case None => AgoraMethodFormat.read(json) // If omitted, default to Agora for backwards compatibility
            case Some(JsString(other)) => throw DeserializationException(s"Illegal method repo \'$other\'")
            case _ => throw DeserializationException("unexpected json type")
          }
      }
    }

  }

  implicit val MethodConfigurationFormat = jsonFormat11(MethodConfiguration)

  implicit val AgoraMethodConfigurationFormat = jsonFormat7(AgoraMethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat4(MethodConfigurationShort)

  implicit val MethodRepoConfigurationImportFormat = jsonFormat4(MethodRepoConfigurationImport)

  implicit val MethodRepoConfigurationExportFormat = jsonFormat3(MethodRepoConfigurationExport)

  implicit val WorkspaceSubmissionStatsFormat = jsonFormat3(WorkspaceSubmissionStats)

  implicit val WorkspaceBucketOptionsFormat = jsonFormat1(WorkspaceBucketOptions)

  implicit val WorkspaceDetailsFormat = jsonFormat11(WorkspaceDetails.apply)

  implicit val WorkspaceListResponseFormat = jsonFormat4(WorkspaceListResponse)

  implicit val WorkspaceResponseFormat = jsonFormat8(WorkspaceResponse)

  implicit val WorkspaceAccessInstructionsFormat = jsonFormat2(ManagedGroupAccessInstructions)

  implicit val ValidatedMethodConfigurationFormat = jsonFormat7(ValidatedMethodConfiguration)

  implicit val GA4GHToolDescriptorFormat = jsonFormat3(GA4GHTool)

  implicit val MethodInputFormat = jsonFormat3(MethodInput)

  implicit val MethodOutputFormat = jsonFormat2(MethodOutput)

  implicit val MethodInputsOutputsFormat = jsonFormat2(MethodInputsOutputs)

  implicit val WorkspaceTagFormat = jsonFormat2(WorkspaceTag)

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

  implicit object ClassFormat extends RootJsonFormat[Class[_]] {
    def write(clazz: Class[_]) =
      JsString(clazz.getName)

    def read(json: JsValue) = json match {
      case JsString(className) => Class.forName(className)
      case _ => throw new DeserializationException("unable to deserialize Class")
    }
  }

  implicit val ErrorReportFormat: RootJsonFormat[ErrorReport] = rootFormat(lazyFormat(jsonFormat(ErrorReport.apply,"source","message","statusCode","causes","stackTrace","exceptionClass")))

  implicit val ApplicationVersionFormat = jsonFormat3(ApplicationVersion)
}

object WorkspaceJsonSupport extends WorkspaceJsonSupport
