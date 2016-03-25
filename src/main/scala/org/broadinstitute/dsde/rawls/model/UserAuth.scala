package org.broadinstitute.dsde.rawls.model

import spray.json._

sealed trait UserAuthRef
case class RawlsUserRef(userSubjectId: RawlsUserSubjectId) extends UserAuthRef
case class RawlsGroupRef(groupName: RawlsGroupName) extends UserAuthRef

sealed trait UserAuthType { val value: String }
case class RawlsUserEmail(value: String) extends UserAuthType
case class RawlsUserSubjectId(value: String) extends UserAuthType
case class RawlsGroupName(value: String) extends UserAuthType
case class RawlsGroupEmail(value: String) extends UserAuthType
case class RawlsBillingProjectName(value: String) extends UserAuthType
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

case class RawlsBillingProject(projectName: RawlsBillingProjectName, users: Set[RawlsUserRef], cromwellAuthBucketUrl: String)

case class SyncReportItem(operation: String, user: Option[RawlsUser], subGroup: Option[RawlsGroupShort], errorReport: Option[ErrorReport])
case class SyncReport(items: Seq[SyncReportItem])

object UserAuthJsonSupport extends JsonSupport {
  trait UserAuthJsonFormatter[T <: UserAuthType] extends RootJsonFormat[T] {
    // TODO: a generic read.  May require reflection.
    override def write(obj: T): JsValue = JsString(obj.value)
  }

  implicit object RawlsUserEmailFormat extends UserAuthJsonFormatter[RawlsUserEmail] {
    override def read(json: JsValue): RawlsUserEmail = json match {
      case JsString(value) => RawlsUserEmail(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }
  }

  implicit object RawlsUserSubjectIdFormat extends UserAuthJsonFormatter[RawlsUserSubjectId] {
    override def read(json: JsValue): RawlsUserSubjectId = json match {
      case JsString(value) => RawlsUserSubjectId(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }
  }

  implicit object RawlsGroupNameFormat extends UserAuthJsonFormatter[RawlsGroupName] {
    override def read(json: JsValue): RawlsGroupName = json match {
      case JsString(value) => RawlsGroupName(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }
  }

  implicit object RawlsGroupEmailFormat extends UserAuthJsonFormatter[RawlsGroupEmail] {
    override def read(json: JsValue): RawlsGroupEmail = json match {
      case JsString(value) => RawlsGroupEmail(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }
  }

  implicit object RawlsBillingProjectNameFormat extends UserAuthJsonFormatter[RawlsBillingProjectName] {
    override def read(json: JsValue): RawlsBillingProjectName = json match {
      case JsString(value) => RawlsBillingProjectName(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }
  }

  // need "apply" here so it doesn't choose the companion class
  implicit val RawlsUserFormat = jsonFormat2(RawlsUser.apply)

  implicit val RawlsUserRefFormat = jsonFormat1(RawlsUserRef)

  implicit val RawlsBillingProjectFormat = jsonFormat3(RawlsBillingProject)

  implicit val RawlsGroupRefFormat = jsonFormat1(RawlsGroupRef)

  implicit val RawlsGroupFormat = jsonFormat4[RawlsGroupName, RawlsGroupEmail, Set[RawlsUserRef], Set[RawlsGroupRef], RawlsGroup](RawlsGroup.apply)

  implicit val RawlsGroupShortFormat = jsonFormat2(RawlsGroupShort)

  implicit val RawlsGroupMemberListFormat = jsonFormat4(RawlsGroupMemberList)

  implicit val RawlsUserInfoFormat = jsonFormat2(RawlsUserInfo)

  implicit val RawlsUserInfoListFormat = jsonFormat1(RawlsUserInfoList)

  import WorkspaceJsonSupport.ErrorReportFormat
  implicit val SyncReportItemFormat = jsonFormat4(SyncReportItem)

  implicit val SyncReportFormat = jsonFormat1(SyncReport)
}