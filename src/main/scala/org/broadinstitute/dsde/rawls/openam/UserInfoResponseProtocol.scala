package org.broadinstitute.dsde.rawls.openam

import spray.json.DefaultJsonProtocol

object UserInfoResponseProtocol extends DefaultJsonProtocol {
  case class UserInfoResponse(username: String, cn: Seq[String], mail: Seq[String])
  implicit val userInfoResponseProtocol = jsonFormat3(UserInfoResponse)
}
