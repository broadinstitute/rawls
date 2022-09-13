package org.broadinstitute.dsde.rawls.model

/**
 * Created by dvoet on 10/27/15.
 */
case class UserIdInfo(userSubjectId: String, userEmail: String, googleSubjectId: Option[String])

case class UserList(userList: Seq[String])

class UserJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val UserIdInfoFormat = jsonFormat3(UserIdInfo)
  implicit val UserListFormat = jsonFormat1(UserList)
}

object UserJsonSupport extends UserJsonSupport
