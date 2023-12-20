package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.model.CloudPlatform.CloudPlatform
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.AzureManagedAppCoordinatesFormat
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.ValueObjectFormat
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import spray.json._

import scala.language.implicitConversions

case class RawlsBillingProjectMembership(projectName: RawlsBillingProjectName,
                                         role: ProjectRoles.ProjectRole,
                                         creationStatus: CreationStatuses.CreationStatus,
                                         message: Option[String] = None
)
case class RawlsBillingProjectStatus(projectName: RawlsBillingProjectName,
                                     creationStatus: CreationStatuses.CreationStatus
)
case class RawlsBillingProjectMember(email: RawlsUserEmail, role: ProjectRoles.ProjectRole)
case class RawlsGroupMemberList(
  userEmails: Option[Seq[String]] = None,
  subGroupEmails: Option[Seq[String]] = None,
  userSubjectIds: Option[Seq[String]] = None,
  subGroupNames: Option[Seq[String]] = None
)
case class RawlsUserInfo(user: RawlsUser, billingProjects: Seq[RawlsBillingProjectName])
case class RawlsUserInfoList(userInfoList: Seq[RawlsUserInfo])

case class RawlsUser(userSubjectId: RawlsUserSubjectId, userEmail: RawlsUserEmail)

object RawlsUser {
  implicit def toRef(u: RawlsUser): RawlsUserRef = RawlsUserRef(u.userSubjectId)

  def apply(userInfo: UserInfo): RawlsUser =
    RawlsUser(userInfo.userSubjectId, userInfo.userEmail)
}

case class RawlsGroup(
  groupName: RawlsGroupName,
  groupEmail: RawlsGroupEmail,
  users: Set[RawlsUserRef],
  subGroups: Set[RawlsGroupRef]
) {
  def toRawlsGroupShort: RawlsGroupShort = RawlsGroupShort(groupName, groupEmail)
}

object RawlsGroup {
  implicit def toRef(g: RawlsGroup): RawlsGroupRef = RawlsGroupRef(g.groupName)
}

trait Managed {
  val membersGroup: RawlsGroup
  val adminsGroup: RawlsGroup
}

object ManagedGroup {
  implicit def toRef(mg: ManagedGroup): ManagedGroupRef = ManagedGroupRef(mg.membersGroup.groupName)
}

case class ManagedGroup(membersGroup: RawlsGroup, adminsGroup: RawlsGroup) extends Managed

case class RawlsBillingAccount(accountName: RawlsBillingAccountName, firecloudHasAccess: Boolean, displayName: String)
case class RawlsBillingProject(
  projectName: RawlsBillingProjectName,
  status: CreationStatuses.CreationStatus,
  billingAccount: Option[RawlsBillingAccountName],
  message: Option[String],
  cromwellBackend: Option[CromwellBackend] = None,
  servicePerimeter: Option[ServicePerimeterName] = None,
  googleProjectNumber: Option[GoogleProjectNumber] = None,
  invalidBillingAccount: Boolean = false,
  spendReportDataset: Option[BigQueryDatasetName] = None,
  spendReportTable: Option[BigQueryTableName] = None,
  spendReportDatasetGoogleProject: Option[GoogleProject] = None,
  azureManagedAppCoordinates: Option[AzureManagedAppCoordinates] = None,
  billingProfileId: Option[String] = None,
  landingZoneId: Option[String] = None
) {
  // def instead of val because val confuses the json formatter
  def googleProjectId: GoogleProjectId = GoogleProjectId(projectName.value)
}

case class WorkspaceBillingAccount(
  workspaceName: WorkspaceName,
  currentBillingAccountOnGoogleProject: Option[RawlsBillingAccountName]
)

case class RawlsBillingProjectResponse(
  projectName: RawlsBillingProjectName,
  billingAccount: Option[RawlsBillingAccountName],
  servicePerimeter: Option[ServicePerimeterName],
  invalidBillingAccount: Boolean,
  roles: Set[ProjectRoles.ProjectRole],
  status: CreationStatuses.CreationStatus,
  message: Option[String],
  managedAppCoordinates: Option[AzureManagedAppCoordinates], // remove after ui is updated  to use cloud context
  cloudPlatform: String,
  landingZoneId: Option[String],
  protectedData: Option[Boolean]
)

object RawlsBillingProjectResponse {
  def apply(
    roles: Set[ProjectRole],
    project: RawlsBillingProject,
    platform: CloudPlatform = CloudPlatform.UNKNOWN,
    protectedData: Option[Boolean] = None
  ): RawlsBillingProjectResponse = this(
    project.projectName,
    project.billingAccount,
    project.servicePerimeter,
    project.invalidBillingAccount,
    roles,
    project.status,
    project.message,
    project.azureManagedAppCoordinates,
    platform.toString,
    project.landingZoneId,
    protectedData
  )
}

case class RawlsBillingProjectTransfer(project: String, bucket: String, newOwnerEmail: String, newOwnerToken: String)

case class ProjectAccessUpdate(email: String, role: ProjectRole)

case class BatchProjectAccessUpdate(membersToAdd: Set[ProjectAccessUpdate], membersToRemove: Set[ProjectAccessUpdate])

object ProjectRoles {
  sealed trait ProjectRole extends RawlsEnumeration[ProjectRole] {
    override def toString: String = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): ProjectRole = ProjectRoles.withName(name)
  }

  def withName(name: String): ProjectRole = name.toLowerCase match {
    case "owner" => Owner
    case "user"  => User
    case _       => throw new RawlsException(s"invalid ProjectRole [${name}]")
  }

  case object Owner extends ProjectRole
  case object User extends ProjectRole

  val all: Set[ProjectRole] = Set(Owner, User)
}

object CreationStatuses {
  sealed trait CreationStatus extends RawlsEnumeration[CreationStatus] {
    override def toString: String = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): CreationStatus = CreationStatuses.withName(name)
  }

  def withName(name: String): CreationStatus = name.toLowerCase match {
    case "creating"            => Creating
    case "ready"               => Ready
    case "error"               => Error
    case "deleting"            => Deleting
    case "deletionfailed"      => DeletionFailed
    case "addingtoperimeter"   => AddingToPerimeter
    case "creatinglandingzone" => CreatingLandingZone
    case _                     => throw new RawlsException(s"invalid CreationStatus [${name}]")
  }

  case object Creating extends CreationStatus
  case object Ready extends CreationStatus
  case object Error extends CreationStatus
  case object Deleting extends CreationStatus
  case object DeletionFailed extends CreationStatus
  case object AddingToPerimeter extends CreationStatus
  case object CreatingLandingZone extends CreationStatus

  val all: Set[CreationStatus] =
    Set(Creating, Ready, Error, Deleting, DeletionFailed, AddingToPerimeter, CreatingLandingZone)
  val terminal: Set[CreationStatus] = Set(Ready, Error, DeletionFailed)
}

// V2 billing projects will have enable flow logs on if in a service perimeter
// Otherwise not. HighSecurityNetwork and PrivateIpGoogleAccess will be on for
// all projects. We do not want flow logs on by default because they are expensive
// and prone to false positives, but for users who are making use of a service
// perimeter, we found that they needed the extra security.
case class CreateRawlsV2BillingProjectFullRequest(
  projectName: RawlsBillingProjectName,
  billingAccount: Option[RawlsBillingAccountName],
  servicePerimeter: Option[ServicePerimeterName],
  managedAppCoordinates: Option[AzureManagedAppCoordinates],
  members: Option[Set[ProjectAccessUpdate]],
  inviteUsersNotFound: Option[Boolean],
  protectedData: Option[Boolean] = Option(false),
  costSavings: Option[Boolean] = Option(false),
) {

  def billingInfo: Either[RawlsBillingAccountName, AzureManagedAppCoordinates] = {
    if (billingAccount.isDefined && managedAppCoordinates.isDefined) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest,
                    "Invalid billing project creation request, only one of azure or gcp billing info is allowed"
        )
      )
    }
    if (billingAccount.isEmpty && managedAppCoordinates.isEmpty) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest,
                    "Invalid billing project creation request, one of azure or gcp billing info is required"
        )
      )
    }
    if (billingAccount.isDefined) {
      Left(billingAccount.get)
    } else {
      Right(managedAppCoordinates.get)
    }
  }
}

case class UpdateRawlsBillingAccountRequest(billingAccount: RawlsBillingAccountName)

case class SyncReportItem(operation: String, email: String, errorReport: Option[ErrorReport])
case class SyncReport(groupEmail: RawlsGroupEmail, items: Seq[SyncReportItem])

case class BillingAccountScopes(requiredScopes: Seq[String])

class UserAuthJsonSupport extends JsonSupport {

  import ExecutionJsonSupport._
  import UserModelJsonSupport._
  import WorkspaceJsonSupport.WorkspaceNameFormat
  import spray.json.DefaultJsonProtocol._

  // need "apply" here so it doesn't choose the companion class
  implicit val RawlsUserFormat: RootJsonFormat[RawlsUser] = jsonFormat2(RawlsUser.apply)

  implicit object ProjectStatusFormat extends RootJsonFormat[CreationStatuses.CreationStatus] {
    override def write(obj: CreationStatuses.CreationStatus): JsValue = JsString(obj.toString)

    override def read(json: JsValue): CreationStatuses.CreationStatus = json match {
      case JsString(name) => CreationStatuses.withName(name)
      case _              => throw DeserializationException("could not deserialize project status")
    }
  }

  implicit object ProjectRoleFormat extends RootJsonFormat[ProjectRole] {
    override def write(obj: ProjectRole): JsValue = JsString(obj.toString)

    override def read(json: JsValue): ProjectRole = json match {
      case JsString(name) => ProjectRoles.withName(name)
      case _              => throw DeserializationException("could not deserialize project role")
    }
  }

  implicit val servicePerimeterNameFormat: ValueObjectFormat[ServicePerimeterName] = ValueObjectFormat(
    ServicePerimeterName
  )

  implicit val googleProjectNumberFormat: ValueObjectFormat[GoogleProjectNumber] = ValueObjectFormat(
    GoogleProjectNumber
  )

  implicit val RawlsGroupFormat: RootJsonFormat[RawlsGroup] =
    jsonFormat4[RawlsGroupName, RawlsGroupEmail, Set[RawlsUserRef], Set[RawlsGroupRef], RawlsGroup](RawlsGroup.apply)

  implicit val RawlsGroupMemberListFormat: RootJsonFormat[RawlsGroupMemberList] = jsonFormat4(RawlsGroupMemberList)

  implicit val RawlsBillingProjectFormat: RootJsonFormat[RawlsBillingProject] = jsonFormat14(RawlsBillingProject)

  implicit val RawlsBillingAccountFormat: RootJsonFormat[RawlsBillingAccount] = jsonFormat3(RawlsBillingAccount)

  implicit val OAuth2BearerTokenFormat: RootJsonFormat[OAuth2BearerToken] = jsonFormat1(OAuth2BearerToken)
  implicit val UserInfoFormat: RootJsonFormat[UserInfo] = jsonFormat5(UserInfo.apply)
  implicit val RawlsBillingProjectTransferFormat: RootJsonFormat[RawlsBillingProjectTransfer] = jsonFormat4(
    RawlsBillingProjectTransfer
  )

  implicit val RawlsUserInfoFormat: RootJsonFormat[RawlsUserInfo] = jsonFormat2(RawlsUserInfo)

  implicit val RawlsUserInfoListFormat: RootJsonFormat[RawlsUserInfoList] = jsonFormat1(RawlsUserInfoList)

  import WorkspaceJsonSupport.ErrorReportFormat

  implicit val SyncReportItemFormat: RootJsonFormat[SyncReportItem] = jsonFormat3(SyncReportItem)

  implicit val SyncReportFormat: RootJsonFormat[SyncReport] = jsonFormat2(SyncReport)

  implicit val ProjectAccessUpdateFormat: RootJsonFormat[ProjectAccessUpdate] = jsonFormat2(ProjectAccessUpdate)

  implicit val BatchProjectAccessUpdateFormat: RootJsonFormat[BatchProjectAccessUpdate] = jsonFormat2(
    BatchProjectAccessUpdate
  )

  implicit val CreateRawlsV2BillingProjectFullRequestFormat: RootJsonFormat[CreateRawlsV2BillingProjectFullRequest] =
    jsonFormat8(CreateRawlsV2BillingProjectFullRequest)

  implicit val UpdateRawlsBillingAccountRequestFormat: RootJsonFormat[UpdateRawlsBillingAccountRequest] = jsonFormat1(
    UpdateRawlsBillingAccountRequest
  )

  implicit val BillingAccountScopesFormat: RootJsonFormat[BillingAccountScopes] = jsonFormat1(BillingAccountScopes)

  implicit val RawlsBillingProjectMembershipFormat: RootJsonFormat[RawlsBillingProjectMembership] = jsonFormat4(
    RawlsBillingProjectMembership
  )

  implicit val RawlsBillingProjectStatusFormat: RootJsonFormat[RawlsBillingProjectStatus] = jsonFormat2(
    RawlsBillingProjectStatus
  )

  implicit val RawlsBillingProjectMemberFormat: RootJsonFormat[RawlsBillingProjectMember] = jsonFormat2(
    RawlsBillingProjectMember
  )

  implicit val WorkspaceBillingAccountFormat: RootJsonFormat[WorkspaceBillingAccount] = jsonFormat2(
    WorkspaceBillingAccount
  )

  implicit val RawlsBillingProjectResponseFormat: RootJsonFormat[RawlsBillingProjectResponse] =
    jsonFormat11(RawlsBillingProjectResponse.apply)
}

object UserAuthJsonSupport extends UserAuthJsonSupport
