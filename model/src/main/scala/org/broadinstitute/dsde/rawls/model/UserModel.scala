package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ManagedRoles.ManagedRole
import org.broadinstitute.dsde.workbench.model.ValueObject
import spray.json._

sealed trait UserAuthRef
case class RawlsUserRef(userSubjectId: RawlsUserSubjectId) extends UserAuthRef
case class RawlsGroupRef(groupName: RawlsGroupName) extends UserAuthRef

object ManagedRoles {
  sealed trait ManagedRole extends RawlsEnumeration[ManagedRole] with Ordered[ManagedRole] {
    override def toString = getClass.getSimpleName.stripSuffix("$")

    override def withName(name: String): ManagedRole = ManagedRoles.withName(name)

    def compare(that: ManagedRole): Int =
      // just do string compare such that admin will be greatest
      that.toString.compareTo(this.toString)
  }

  def withName(name: String): ManagedRole = name.toLowerCase match {
    case "admin"  => Admin
    case "member" => Member
    case _        => throw new RawlsException(s"invalid role [${name}]")
  }

  case object Admin extends ManagedRole
  case object Member extends ManagedRole

  val all: Set[ManagedRole] = Set(Admin, Member)
}

case class ManagedGroupRef(membersGroupName: RawlsGroupName) extends UserAuthRef {
  def toMembersGroupRef: RawlsGroupRef = RawlsGroupRef(membersGroupName)
}
case class RawlsGroupShort(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail)
case class ManagedGroupAccess(managedGroupRef: ManagedGroupRef, role: ManagedRole)
case class ManagedGroupAccessResponse(groupName: RawlsGroupName, groupEmail: RawlsGroupEmail, role: ManagedRole)
case class ManagedGroupWithMembers(membersGroup: RawlsGroupShort, membersEmails: Seq[String], adminsEmails: Seq[String])

sealed trait UserAuthType { val value: String }
case class RawlsUserEmail(value: String) extends UserAuthType
case class RawlsUserSubjectId(value: String) extends UserAuthType
case class RawlsGroupName(value: String) extends UserAuthType {
  // ignore case on equals and hashcode
  override def hashCode(): Int = value.toLowerCase.hashCode
  override def equals(other: scala.Any): Boolean =
    other match {
      case RawlsGroupName(otherValue) => value.equalsIgnoreCase(otherValue)
      case _                          => false
    }
}
case class RawlsGroupEmail(value: String) extends UserAuthType
case class RawlsBillingAccountName(value: String) extends UserAuthType {
  def withoutPrefix(): String = value.stripPrefix("billingAccounts/")
}
case class RawlsBillingProjectName(value: String) extends UserAuthType

class UserModelJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  case class UserModelJsonFormatter[T <: UserAuthType](create: String => T) extends RootJsonFormat[T] {
    def read(obj: JsValue): T = obj match {
      case JsString(value) => create(value)
      case _               => throw new DeserializationException("could not deserialize user object")
    }

    def write(obj: T): JsValue = JsString(obj.value)
  }

  implicit object ManagedRoleFormat extends RootJsonFormat[ManagedRole] {
    override def write(obj: ManagedRole): JsValue = JsString(obj.toString)

    override def read(json: JsValue): ManagedRole = json match {
      case JsString(name) => ManagedRoles.withName(name)
      case _              => throw new DeserializationException("could not deserialize managed role")
    }
  }

  implicit val RawlsUserEmailFormat: UserModelJsonFormatter[RawlsUserEmail] = UserModelJsonFormatter(RawlsUserEmail)
  implicit val RawlsUserSubjectIdFormat: UserModelJsonFormatter[RawlsUserSubjectId] = UserModelJsonFormatter(
    RawlsUserSubjectId
  )

  implicit val RawlsGroupNameFormat: UserModelJsonFormatter[RawlsGroupName] = UserModelJsonFormatter(RawlsGroupName)
  implicit val RawlsGroupEmailFormat: UserModelJsonFormatter[RawlsGroupEmail] = UserModelJsonFormatter(RawlsGroupEmail)
  implicit val RawlsBillingAccountNameFormat: UserModelJsonFormatter[RawlsBillingAccountName] = UserModelJsonFormatter(
    RawlsBillingAccountName
  )
  implicit val RawlsBillingProjectNameFormat: UserModelJsonFormatter[RawlsBillingProjectName] = UserModelJsonFormatter(
    RawlsBillingProjectName
  )

  implicit val RawlsUserRefFormat: RootJsonFormat[RawlsUserRef] = jsonFormat1(RawlsUserRef)
  implicit val RawlsGroupRefFormat: RootJsonFormat[RawlsGroupRef] = jsonFormat1(RawlsGroupRef)
  implicit val RawlsGroupShortFormat: RootJsonFormat[RawlsGroupShort] = jsonFormat2(RawlsGroupShort)
  implicit val ManagedGroupRefFormat: RootJsonFormat[ManagedGroupRef] = jsonFormat1(ManagedGroupRef)
  implicit val ManagedGroupAccessFormat: RootJsonFormat[ManagedGroupAccess] = jsonFormat2(ManagedGroupAccess)
  implicit val ManagedGroupAccessResponseFormat: RootJsonFormat[ManagedGroupAccessResponse] = jsonFormat3(
    ManagedGroupAccessResponse
  )
  implicit val ManagedGroupWithMembersFormat: RootJsonFormat[ManagedGroupWithMembers] = jsonFormat3(
    ManagedGroupWithMembers
  )
}

object UserModelJsonSupport extends UserModelJsonSupport

case class ServicePerimeterName(value: String) extends ValueObject

case class GoogleProjectNumber(value: String) extends ValueObject
