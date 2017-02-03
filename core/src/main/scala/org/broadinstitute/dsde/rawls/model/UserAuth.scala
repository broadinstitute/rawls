package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import spray.json._

case class RawlsBillingProjectMembership(projectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole, creationStatus: CreationStatuses.CreationStatus)
case class RawlsBillingProjectMember(email: RawlsUserEmail, role: ProjectRoles.ProjectRole)
case class RawlsGroupMemberList(userEmails: Option[Seq[String]] = None, subGroupEmails: Option[Seq[String]] = None, userSubjectIds: Option[Seq[String]] = None, subGroupNames: Option[Seq[String]] = None)
case class RawlsUserInfo(user: RawlsUser, billingProjects: Seq[RawlsBillingProjectName])
case class RawlsUserInfoList(userInfoList: Seq[RawlsUserInfo])

case class RawlsUser(userSubjectId: RawlsUserSubjectId, userEmail: RawlsUserEmail)

object RawlsUser {
  implicit def toRef(u: RawlsUser) = RawlsUserRef(u.userSubjectId)

  def apply(userInfo: UserInfo): RawlsUser =
    RawlsUser(RawlsUserSubjectId(userInfo.userSubjectId), RawlsUserEmail(userInfo.userEmail))
}

case class RawlsGroup(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]) {
  def toRawlsGroupShort = RawlsGroupShort(groupName, groupEmail)
}

object RawlsGroup {
  implicit def toRef(g: RawlsGroup) = RawlsGroupRef(g.groupName)
}

case class RawlsGroupShort(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail)

case class RawlsBillingAccount(accountName: RawlsBillingAccountName, firecloudHasAccess: Boolean, displayName: String)
case class RawlsBillingProject(projectName: RawlsBillingProjectName, groups: Map[ProjectRoles.ProjectRole, RawlsGroup], cromwellAuthBucketUrl: String, status: CreationStatuses.CreationStatus, billingAccount: Option[RawlsBillingAccountName])

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
    case _ => throw new RawlsException(s"invalid CreationStatus [${name}]")
  }

  case object Creating extends CreationStatus
  case object Ready extends CreationStatus

  val all: Set[CreationStatus] = Set(Creating, Ready)
}

case class CreateRawlsBillingProjectFullRequest(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName)

case class SyncReportItem(operation: String, email: String, errorReport: Option[CErrorReport])
case class SyncReport(groupEmail: RawlsGroupEmail, items: Seq[SyncReportItem])

case class BillingAccountScopes(requiredScopes: Seq[String])

object UserAuthJsonSupport extends JsonSupport {
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

  implicit val RawlsGroupShortFormat = jsonFormat2(RawlsGroupShort)

  implicit val RawlsGroupMemberListFormat = jsonFormat4(RawlsGroupMemberList)

  implicit val RawlsBillingProjectFormat = jsonFormat5(RawlsBillingProject)

  implicit val RawlsBillingAccountFormat = jsonFormat3(RawlsBillingAccount)

  implicit val RawlsUserInfoFormat = jsonFormat2(RawlsUserInfo)

  implicit val RawlsUserInfoListFormat = jsonFormat1(RawlsUserInfoList)

  import WorkspaceJsonSupport.ErrorReportFormat
  implicit val SyncReportItemFormat = jsonFormat3(SyncReportItem)

  implicit val SyncReportFormat = jsonFormat2(SyncReport)

  implicit val CreateRawlsBillingProjectFullRequestFormat = jsonFormat2(CreateRawlsBillingProjectFullRequest)

  implicit val BillingAccountScopesFormat = jsonFormat1(BillingAccountScopes)

  implicit val RawlsBillingProjectMembershipFormat = jsonFormat3(RawlsBillingProjectMembership)

  implicit val RawlsBillingProjectMemberFormat = jsonFormat2(RawlsBillingProjectMember)

  implicit val ProjectAccessUpdateFormat = jsonFormat2(ProjectAccessUpdate)
}
