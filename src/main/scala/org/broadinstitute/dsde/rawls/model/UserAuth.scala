package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._

sealed trait UserAuthRef
case class RawlsUserRef(userSubjectId: RawlsUserSubjectId) extends UserAuthRef
case class RawlsGroupRef(groupName: RawlsGroupName) extends UserAuthRef

sealed trait UserAuthType { val value: String }
case class RawlsUserEmail(value: String) extends UserAuthType
case class RawlsUserSubjectId(value: String) extends UserAuthType
case class RawlsGroupName(value: String) extends UserAuthType
case class RawlsGroupEmail(value: String) extends UserAuthType

case class RawlsUser(userSubjectId: RawlsUserSubjectId, userEmail: RawlsUserEmail) extends DomainObject {
  def idFields = Seq("userSubjectId")
}

object RawlsUser {
  implicit def toRef(u: RawlsUser) = RawlsUserRef(u.userSubjectId)

  def apply(userInfo: UserInfo): RawlsUser =
    RawlsUser(RawlsUserSubjectId(userInfo.userSubjectId), RawlsUserEmail(userInfo.userEmail))
}

case class RawlsGroup(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]) extends DomainObject {
  def idFields = Seq("groupName")
}

object RawlsGroup {
  implicit def toRef(g: RawlsGroup) = RawlsGroupRef(g.groupName)

  // for Workspace Access Groups
  def apply(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel): RawlsGroup =
    apply(workspaceName, accessLevel, Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

  // for Workspace Access Groups
  def apply(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef], groups: Set[RawlsGroupRef]): RawlsGroup = {
    val name = RawlsGroupName(UserAuth.toWorkspaceAccessGroupName(workspaceName, accessLevel))
    RawlsGroup(name, RawlsGroupEmail(""), users, groups)
  }
}


object UserAuth {

  def toWorkspaceAccessGroupName(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) =
    s"rawls ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}"

}

object UserAuthJsonSupport extends JsonSupport {
  implicit val RawlsUserEmailFormat = jsonFormat1(RawlsUserEmail)

  implicit val RawlsUserSubjectIdFormat = jsonFormat1(RawlsUserSubjectId)

  implicit val RawlsGroupNameFormat = jsonFormat1(RawlsGroupName)

  implicit val RawlsGroupEmailFormat = jsonFormat1(RawlsGroupEmail)

  // need "apply" here so it doesn't choose the companion class
  implicit val RawlsUserFormat = jsonFormat2(RawlsUser.apply)

  implicit val RawlsUserRefFormat = jsonFormat1(RawlsUserRef)

  implicit val RawlsGroupRefFormat = jsonFormat1(RawlsGroupRef)

  implicit val RawlsGroupFormat = jsonFormat4[RawlsGroupName, RawlsGroupEmail, Set[RawlsUserRef], Set[RawlsGroupRef], RawlsGroup](RawlsGroup.apply)
}