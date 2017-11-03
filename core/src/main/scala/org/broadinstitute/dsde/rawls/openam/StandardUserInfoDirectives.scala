package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.dataaccess.{HttpSamDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserRef, RawlsUserSubjectId, UserInfo, UserStatus}
import spray.http.{HttpHeader, OAuth2BearerToken}
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.Future

trait StandardUserInfoDirectives extends UserInfoDirectives with JndiDirectoryDAO {

  val httpSamDAO: SamDAO

  //TODO: project should be regex
  val petSAdomain = "\\S+@\\S+.iam.gserviceaccount.com"
  val petEmailPrefix = "pet-"
  def requireUserInfo(): Directive1[UserInfo] = {

    val userInfo = for(
      accessToken <- accessTokenHeaderDirective;
      userEmail <- emailHeaderDirective;
      accessTokenExpiresIn <- accessTokenExpiresInHeaderDirective;
      userSubjectId <- userSubjectIdDirective
    ) yield UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(accessToken), accessTokenExpiresIn.toLong, RawlsUserSubjectId(userSubjectId))

    userInfo flatMap { ui =>
      onSuccess(getWorkbenchUserEmailId(ui)).map {
        case Some(resourceType) => UserInfo(resourceType.userInfo.userEmail, ui.accessToken, ui.accessTokenExpiresIn, resourceType.userInfo.userSubjectId)
        case None => UserInfo(ui.userEmail, ui.accessToken, ui.accessTokenExpiresIn, ui.userSubjectId)
      }
    }
  }
  private def isPetSA(email:String) = email.matches(petSAdomain)
  
  private def getWorkbenchUserEmailId(email:UserInfo):Future[Option[UserStatus]] = {
    if (isPetSA(email.userEmail.value))
      httpSamDAO.getUserStatus(email)
    else
      Future(None)
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
