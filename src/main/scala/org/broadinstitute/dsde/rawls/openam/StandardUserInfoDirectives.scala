package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http.HttpHeaders.Authorization
import spray.http.{OAuth2BearerToken, HttpHeaders, HttpCookie}
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.ExecutionContext

trait StandardUserInfoDirectives extends UserInfoDirectives {

  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    implicit val ec = magnet.value
    for(accessToken <- accessTokenHeaderDirective;
        userEmail <- emailHeaderDirective;
        accessTokenExpires <- accessTokenExpiresHeaderDirective
    ) yield UserInfo(userEmail, OAuth2BearerToken(accessToken), accessTokenExpires.toLong)
  }

  private def accessTokenHeaderDirective: Directive1[String] = headerValueByName("OIDC_access_token")
  private def accessTokenExpiresHeaderDirective: Directive1[String] = headerValueByName("OIDC_access_token_expires")
  private def emailHeaderDirective: Directive1[String] = headerValueByName("OIDC_CLAIM_email")
}
