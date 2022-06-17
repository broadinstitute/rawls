package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1}
import akka.http.scaladsl.server.Directives.{headerValueByName, onSuccess, optionalHeaderValueByName, reject}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUser, RawlsUserEmail, RawlsUserSubjectId, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

trait StandardUserInfoDirectives extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext
  val samDAO: SamDAO

  val serviceAccountDomain = "\\S+@\\S+\\.iam\\.gserviceaccount\\.com".r

  private def isServiceAccount(email: String) = {
    serviceAccountDomain.pattern.matcher(email).matches
  }

  /**
   * Returns a UserInfo object, pulled from SAM if present, falling back to the OIDC token headers.
   */
  def requireUserInfo(): Directive1[UserInfo] = (
    headerValueByName("OIDC_access_token") &
      headerValueByName("OIDC_CLAIM_user_id") &
      headerValueByName("OIDC_CLAIM_expires_in") &
      headerValueByName("OIDC_CLAIM_email") &
      optionalHeaderValueByName("OAUTH2_CLAIM_idp_access_token")
    ) tflatMap {
    case (token, userId, expiresIn, email, googleTokenOpt) =>
      val userInfo = UserInfo(RawlsUserEmail(email), OAuth2BearerToken(token), expiresIn.toLong, RawlsUserSubjectId(userId), googleTokenOpt.map(OAuth2BearerToken))
      onSuccess(samDAO.getUserStatus(userInfo).map {
        case Some(samUser) =>
          if (!samUser.enabled) {
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "User not authorized"))
          } else {
            if (isServiceAccount(userInfo.userEmail.value)) {
              userInfo.copy(userEmail = samUser.userEmail, userSubjectId = samUser.userSubjectId)
            } else {
              userInfo
            }
          }
        case None =>  userInfo
      })
  }
}
