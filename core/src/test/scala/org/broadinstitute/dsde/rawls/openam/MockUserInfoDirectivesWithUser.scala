package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.{RawlsUser, UserInfo}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._

/**
  * Created by ursas on 5/3/17.
  */
trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
  val user: RawlsUser
  def requireUserInfo(): Directive1[UserInfo] = {
    // just return the cookie text as the common name
    provide(UserInfo(user.userEmail, OAuth2BearerToken("token"), 123, user.userSubjectId))
  }
}
