package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime
import UserAuthJsonSupport._
import spray.httpx.SprayJsonSupport._

/**
 * Created by dvoet on 10/27/15.
 */
case class UserRefreshToken(refreshToken: String)
case class UserRefreshTokenDate(refreshTokenUpdatedDate: DateTime)

// there are a couple systems we check for enabled status, google and ldap, the enabled map below has an entry for each
case class UserStatus(userInfo: RawlsUser, enabled: Map[String, Boolean])

case class UserList(userList: Seq[String])

object UserJsonSupport extends JsonSupport {
  implicit val UserRefreshTokenFormat = jsonFormat1(UserRefreshToken)
  implicit val UserRefreshTokenDateFormat = jsonFormat1(UserRefreshTokenDate)
  implicit val UserStatusFormat = jsonFormat2(UserStatus)
  implicit val UserListFormat = jsonFormat1(UserList)
}