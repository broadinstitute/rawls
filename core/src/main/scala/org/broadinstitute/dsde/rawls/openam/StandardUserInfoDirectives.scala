package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo, UserStatus}
import spray.http.{HttpHeader, OAuth2BearerToken}
import spray.routing.Directive1
import spray.routing.Directives._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

trait StandardUserInfoDirectives extends UserInfoDirectives {

  val samDAO: SamDAO


  val serviceAccountDomain = "\\S+@\\S+\\.iam\\.gserviceaccount\\.com".r

  private def isServiceAccount(email: String) = {
    serviceAccountDomain.pattern.matcher(email).matches
  }


  def requireUserInfo(): Directive1[UserInfo] = detach(()) & {
    val userInfo = for {
      accessToken <- accessTokenHeaderDirective
      userEmail <- emailHeaderDirective
      accessTokenExpiresIn <- accessTokenExpiresInHeaderDirective
      userSubjectId <- userSubjectIdDirective
    } yield UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(accessToken), accessTokenExpiresIn.toLong, RawlsUserSubjectId(userSubjectId))

    userInfo flatMap { ui =>
      onSuccess(getWorkbenchUserEmailId(ui)).map {
        case Some(samUserInfo) => UserInfo(samUserInfo.userInfo.userEmail, ui.accessToken, ui.accessTokenExpiresIn, samUserInfo.userInfo.userSubjectId)
        case None => UserInfo(ui.userEmail, ui.accessToken, ui.accessTokenExpiresIn, ui.userSubjectId)
      }
    }
  }

  private def getWorkbenchUserEmailId(userInfo:UserInfo):Future[Option[UserStatus]] = {
    if (isServiceAccount(userInfo.userEmail.value)) {
      samDAO.getUserStatus(userInfo)
    }
    else {
      Future.successful(None)
    }
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
