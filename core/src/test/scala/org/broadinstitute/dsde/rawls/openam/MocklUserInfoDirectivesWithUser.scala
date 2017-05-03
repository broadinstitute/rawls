package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.{RawlsUser, UserInfo}
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http.OAuth2BearerToken
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.ExecutionContext

/**
  * Created by ursas on 5/3/17.
  */
trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
  val user: RawlsUser
  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    // just return the cookie text as the common name
    provide(UserInfo(user.userEmail.value, OAuth2BearerToken("token"), 123, user.userSubjectId.value))
  }
}
