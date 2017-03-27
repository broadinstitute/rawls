package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http.OAuth2BearerToken
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.ExecutionContext

trait MockUserInfoDirectives extends UserInfoDirectives {
  protected def userInfo = UserInfo("owner-access", OAuth2BearerToken("token"), 123, "123456789876543212345")

  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    provide(userInfo)
  }
}

trait MockAdminUserInfoDirectives extends UserInfoDirectives {
  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    // just return the cookie text as the common name
    provide(UserInfo("admin", OAuth2BearerToken("token"), 123, "123456789876543212345"))
  }
}
