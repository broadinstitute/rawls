package org.broadinstitute.dsde.rawls.model

import com.google.api.client.auth.oauth2.Credential
import spray.http.OAuth2BearerToken

/**
 * Created by dvoet on 7/21/15.
 */
case class UserInfo(userEmail: RawlsUserEmail, accessToken: OAuth2BearerToken, tokenExpiresIn: Long, userId: RawlsUserSubjectId)

object UserInfo {
  def buildFromTokens(credential: Credential): UserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), RawlsUserSubjectId(""))
  }
}