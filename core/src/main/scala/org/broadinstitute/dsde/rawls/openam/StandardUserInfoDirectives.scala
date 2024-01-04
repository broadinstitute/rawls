package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.{headerValueByName, onSuccess, optionalHeaderValueByName}
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, SamUserStatusResponse, UserInfo}

import scala.concurrent.{ExecutionContext, Future}

trait StandardUserInfoDirectives extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext
  val samDAO: SamDAO

  val serviceAccountDomain = "\\S+@\\S+\\.iam\\.gserviceaccount\\.com".r

  private def isServiceAccount(email: String) =
    serviceAccountDomain.pattern.matcher(email).matches

  def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] = (
    headerValueByName("OIDC_access_token") &
      headerValueByName("OIDC_CLAIM_user_id") &
      headerValueByName("OIDC_CLAIM_expires_in") &
      headerValueByName("OIDC_CLAIM_email") &
      optionalHeaderValueByName("OAUTH2_CLAIM_idp_access_token")
  ) tflatMap { case (token, userId, expiresIn, email, googleTokenOpt) =>
    val userInfo = UserInfo(RawlsUserEmail(email),
                            OAuth2BearerToken(token),
                            expiresIn.toLong,
                            RawlsUserSubjectId(userId),
                            googleTokenOpt.map(OAuth2BearerToken)
    )
    onSuccess(getWorkbenchUserEmailId(RawlsRequestContext(userInfo, otelContext)).map {
      case Some(petOwnerUser) =>
        userInfo.copy(userEmail = RawlsUserEmail(petOwnerUser.userEmail),
                      userSubjectId = RawlsUserSubjectId(petOwnerUser.userSubjectId)
        )
      case None => userInfo
    })
  }

  private def getWorkbenchUserEmailId(ctx: RawlsRequestContext): Future[Option[SamUserStatusResponse]] =
    if (isServiceAccount(ctx.userInfo.userEmail.value)) {
      samDAO.getUserStatus(ctx)
    } else {
      Future.successful(None)
    }
}
