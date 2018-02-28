package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ManagedRoles.ManagedRole
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import spray.json._

case class RawlsBillingProjectMembership(projectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole, creationStatus: CreationStatuses.CreationStatus, message: Option[String] = None)
case class RawlsBillingProjectMember(email: RawlsUserEmail, role: ProjectRoles.ProjectRole)
case class RawlsGroupMemberList(userEmails: Option[Seq[String]] = None, subGroupEmails: Option[Seq[String]] = None, userSubjectIds: Option[Seq[String]] = None, subGroupNames: Option[Seq[String]] = None)
case class RawlsUserInfo(user: RawlsUser, billingProjects: Seq[RawlsBillingProjectName])
case class RawlsUserInfoList(userInfoList: Seq[RawlsUserInfo])

case class RawlsUser(userSubjectId: RawlsUserSubjectId, userEmail: RawlsUserEmail)

object RawlsUser {
  implicit def toRef(u: RawlsUser): RawlsUserRef = RawlsUserRef(u.userSubjectId)

  def apply(userInfo: UserInfo): RawlsUser =
    RawlsUser(userInfo.userSubjectId, userInfo.userEmail)
}

case class RawlsGroup(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]) {
  def toRawlsGroupShort = RawlsGroupShort(groupName, groupEmail)
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
case class RawlsBillingProject(projectName: RawlsBillingProjectName, cromwellAuthBucketUrl: String, status: CreationStatuses.CreationStatus, billingAccount: Option[RawlsBillingAccountName], message: Option[String])

case class RawlsBillingProjectTransfer(project: String, bucket: String, newOwnerEmail: String, newOwnerToken: String)

case class ProjectAccessUpdate(email: String, role: ProjectRole)

object ProjectRoles {
  sealed trait ProjectRole extends RawlsEnumeration[ProjectRole] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): ProjectRole = ProjectRoles.withName(name)
  }

  def withName(name: String): ProjectRole = name.toLowerCase match {
    case "owner" => Owner
    case "user" => User
    case _ => throw new RawlsException(s"invalid ProjectRole [${name}]")
  }

  case object Owner extends ProjectRole
  case object User extends ProjectRole

  val all: Set[ProjectRole] = Set(Owner, User)
}

object CreationStatuses {
  sealed trait CreationStatus extends RawlsEnumeration[CreationStatus] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): CreationStatus = CreationStatuses.withName(name)
  }

  def withName(name: String): CreationStatus = name.toLowerCase match {
    case "creating" => Creating
    case "ready" => Ready
    case "error" => Error
    case _ => throw new RawlsException(s"invalid CreationStatus [${name}]")
  }

  case object Creating extends CreationStatus
  case object Ready extends CreationStatus
  case object Error extends CreationStatus

  val all: Set[CreationStatus] = Set(Creating, Ready, Error)
  val terminal: Set[CreationStatus] = Set(Ready, Error)
}

case class CreateRawlsBillingProjectFullRequest(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName)

case class SyncReportItem(operation: String, email: String, errorReport: Option[ErrorReport])
case class SyncReport(groupEmail: RawlsGroupEmail, items: Seq[SyncReportItem])

case class BillingAccountScopes(requiredScopes: Seq[String])

class UserAuthJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._
  import UserModelJsonSupport._

  // need "apply" here so it doesn't choose the companion class
  implicit val RawlsUserFormat = jsonFormat2(RawlsUser.apply)

  implicit object ProjectStatusFormat extends RootJsonFormat[CreationStatuses.CreationStatus] {
    override def write(obj: CreationStatuses.CreationStatus): JsValue = JsString(obj.toString)

    override def read(json: JsValue): CreationStatuses.CreationStatus = json match {
      case JsString(name) => CreationStatuses.withName(name)
      case _ => throw new DeserializationException("could not deserialize project status")
    }
  }

  implicit object ProjectRoleFormat extends RootJsonFormat[ProjectRole] {
    override def write(obj: ProjectRole): JsValue = JsString(obj.toString)

    override def read(json: JsValue): ProjectRole = json match {
      case JsString(name) => ProjectRoles.withName(name)
      case _ => throw new DeserializationException("could not deserialize project role")
    }
  }

  implicit val RawlsGroupFormat = jsonFormat4[RawlsGroupName, RawlsGroupEmail, Set[RawlsUserRef], Set[RawlsGroupRef], RawlsGroup](RawlsGroup.apply)

  implicit val RawlsGroupMemberListFormat = jsonFormat4(RawlsGroupMemberList)

  implicit val RawlsBillingProjectFormat = jsonFormat5(RawlsBillingProject)

  implicit val RawlsBillingAccountFormat = jsonFormat3(RawlsBillingAccount)

  implicit val OAuth2BearerTokenFormat = jsonFormat1(OAuth2BearerToken)
  implicit val UserInfoFormat = jsonFormat4(UserInfo.apply)
  implicit val RawlsBillingProjectTransferFormat = jsonFormat4(RawlsBillingProjectTransfer)

  implicit val RawlsUserInfoFormat = jsonFormat2(RawlsUserInfo)

  implicit val RawlsUserInfoListFormat = jsonFormat1(RawlsUserInfoList)

  import WorkspaceJsonSupport.ErrorReportFormat
  implicit val SyncReportItemFormat = jsonFormat3(SyncReportItem)

  implicit val SyncReportFormat = jsonFormat2(SyncReport)

  implicit val CreateRawlsBillingProjectFullRequestFormat = jsonFormat2(CreateRawlsBillingProjectFullRequest)

  implicit val BillingAccountScopesFormat = jsonFormat1(BillingAccountScopes)

  implicit val RawlsBillingProjectMembershipFormat = jsonFormat4(RawlsBillingProjectMembership)

  implicit val RawlsBillingProjectMemberFormat = jsonFormat2(RawlsBillingProjectMember)

  implicit val ProjectAccessUpdateFormat = jsonFormat2(ProjectAccessUpdate)
}

object UserAuthJsonSupport extends UserAuthJsonSupport