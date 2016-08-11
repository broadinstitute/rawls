package org.broadinstitute.dsde.rawls.model

import com.google.api.client.auth.oauth2.Credential
import spray.http.OAuth2BearerToken

/**
 * Created by dvoet on 7/21/15.
 */
case class UserInfo(userEmail: String, accessToken: OAuth2BearerToken, accessTokenExpiresIn: Long, userSubjectId: String)

object UserInfo {
  def buildFromTokens(credential: Credential): UserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    // TODO: alternate expiration test
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo("", OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), "")
  }
}