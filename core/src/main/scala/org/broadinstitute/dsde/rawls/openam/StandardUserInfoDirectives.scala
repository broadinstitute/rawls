package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import spray.http.{HttpHeader, OAuth2BearerToken}
import spray.routing.Directive1
import spray.routing.Directives._

trait StandardUserInfoDirectives extends UserInfoDirectives {

  def requireUserInfo(): Directive1[UserInfo] = {
    for(accessToken <- accessTokenHeaderDirective;
        userEmail <- emailHeaderDirective;
        accessTokenExpiresIn <- accessTokenExpiresInHeaderDirective;
        userSubjectId <- userSubjectIdDirective
    ) yield UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(accessToken), accessTokenExpiresIn.toLong, RawlsUserSubjectId(userSubjectId))
  }

  private def accessTokenHeaderDirective: Directive1[String] = headerValueByName("OIDC_access_token")
  private def accessTokenExpiresInHeaderDirective: Directive1[String] = headerValueByName("OIDC_CLAIM_expires_in")
  private def emailHeaderDirective: Directive1[String] = headerValueByName("OIDC_CLAIM_email")
  private def userSubjectIdDirective: Directive1[String] = headerValue(extractUniqueId)

  def extractUniqueId: HttpHeader => Option[String] = {hdr:HttpHeader =>
    hdr.name match {
      case "OIDC_CLAIM_sub" => Some(hdr.value)
      case "OIDC_CLAIM_user_id" => Some(hdr.value)
      case _ => None
    }
  }
}
