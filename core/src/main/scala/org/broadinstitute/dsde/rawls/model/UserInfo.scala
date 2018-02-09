package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.Credential

/**
 * Created by dvoet on 7/21/15.
 */
case class UserInfo(userEmail: RawlsUserEmail, accessToken: OAuth2BearerToken, accessTokenExpiresIn: Long, userSubjectId: RawlsUserSubjectId)

object UserInfo {
  def buildFromTokens(credential: Credential): UserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), RawlsUserSubjectId(""))
  }
}