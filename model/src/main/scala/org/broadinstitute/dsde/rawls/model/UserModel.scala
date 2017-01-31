package org.broadinstitute.dsde.rawls.model

import spray.json._

sealed trait UserAuthRef
case class RawlsUserRef(userSubjectId: RawlsUserSubjectId) extends UserAuthRef
case class RawlsGroupRef(groupName: RawlsGroupName) extends UserAuthRef
case class RawlsRealmRef(realmName: RawlsGroupName) extends UserAuthRef

object RawlsGroupRef {
  implicit def toRealmRef(ref: RawlsGroupRef) = RawlsRealmRef(ref.groupName)
}

object RawlsRealmRef {
  implicit def toGroupRef(ref: RawlsRealmRef) = RawlsGroupRef(ref.realmName)
}

sealed trait UserAuthType { val value: String }
case class RawlsUserEmail(value: String) extends UserAuthType
case class RawlsUserSubjectId(value: String) extends UserAuthType
case class RawlsGroupName(value: String) extends UserAuthType
case class RawlsGroupEmail(value: String) extends UserAuthType
case class RawlsBillingAccountName(value: String) extends UserAuthType
case class RawlsBillingProjectName(value: String) extends UserAuthType

object UserModelJsonSupport extends JsonSupport {

  case class UserModelJsonFormatter[T <: UserAuthType](create: String => T) extends RootJsonFormat[T] {
    def read(obj: JsValue): T = obj match {
      case JsString(value) => create(value)
      case _ => throw new DeserializationException("could not deserialize user object")
    }

    def write(obj: T): JsValue = JsString(obj.value)
  }

  implicit val RawlsUserEmailFormat = UserModelJsonFormatter(RawlsUserEmail)
  implicit val RawlsUserSubjectIdFormat = UserModelJsonFormatter(RawlsUserSubjectId)

  implicit val RawlsGroupNameFormat = UserModelJsonFormatter(RawlsGroupName)
  implicit val RawlsGroupEmailFormat = UserModelJsonFormatter(RawlsGroupEmail)
  implicit val RawlsBillingAccountNameFormat = UserModelJsonFormatter(RawlsBillingAccountName)
  implicit val RawlsBillingProjectNameFormat = UserModelJsonFormatter(RawlsBillingProjectName)

  implicit val RawlsUserRefFormat = jsonFormat1(RawlsUserRef)
  implicit val RawlsGroupRefFormat = jsonFormat1(RawlsGroupRef.apply)
  implicit val RawlsRealmRefFormat = jsonFormat1(RawlsRealmRef.apply)
}