package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._

sealed trait UserAuthRef
case class RawlsUserRef(userSubjectId: String) extends UserAuthRef
case class RawlsGroupRef(groupName: String) extends UserAuthRef

case class RawlsUser(userSubjectId: String) extends DomainObject {
  def idFields = Seq("userSubjectId")
}

object RawlsUser {
  implicit def toRef(u: RawlsUser) = RawlsUserRef(u.userSubjectId)
}

case class RawlsGroup(groupName: String, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]) extends DomainObject {
  def idFields = Seq("groupName")
}

object RawlsGroup {
  implicit def toRef(g: RawlsGroup) = RawlsGroupRef(g.groupName)
}


object UserAuth {

  def toWorkspaceAccessGroupName(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) =
    s"rawls ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}"

}

object UserAuthJsonSupport extends JsonSupport {
  // need "apply" here so it doesn't choose the companion class
  implicit val RawlsUserFormat = jsonFormat1(RawlsUser.apply)

  implicit val RawlsUserRefFormat = jsonFormat1(RawlsUserRef)

  implicit val RawlsGroupRefFormat = jsonFormat1(RawlsGroupRef)

  implicit val RawlsGroupFormat = jsonFormat3(RawlsGroup.apply)
}