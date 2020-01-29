package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime

/**
 * Created by dvoet on 10/27/15.
 */
case class UserRefreshToken(refreshToken: String)
case class UserRefreshTokenDate(refreshTokenUpdatedDate: DateTime)

case class UserIdInfo(userSubjectId: String, userEmail: String, googleSubjectId: Option[String])

case class UserList(userList: Seq[String])

class UserJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val UserRefreshTokenFormat = jsonFormat1(UserRefreshToken)
  implicit val UserRefreshTokenDateFormat = jsonFormat1(UserRefreshTokenDate)
  implicit val UserIdInfoFormat = jsonFormat3(UserIdInfo)
  implicit val UserListFormat = jsonFormat1(UserList)
}

object UserJsonSupport extends UserJsonSupport
