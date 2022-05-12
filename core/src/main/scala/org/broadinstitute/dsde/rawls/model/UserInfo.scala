package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.Credential
import com.google.auth.oauth2.GoogleCredentials

/**
  * Represents an authenticated Rawls user.
  * @param userEmail the user's email address.
  * @param accessToken the user's access token.
  * @param accessTokenExpiresIn number of seconds until the access token expires.
  * @param userSubjectId the user id.
  * @param idpAccessToken if the request came through B2C, optional access token of the
  *                       underlying identity provider (e.g. Google opaque token).
  */
case class UserInfo(userEmail: RawlsUserEmail,
                    accessToken: OAuth2BearerToken,
                    accessTokenExpiresIn: Long,
                    userSubjectId: RawlsUserSubjectId,
                    idpAccessToken: Option[OAuth2BearerToken] = None)

object UserInfo {
  def buildFromTokens(credential: Credential): UserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), RawlsUserSubjectId(""))
  }

  def buildFromTokens(gCredential: GoogleCredentials): UserInfo = {
    val aToken = gCredential.refreshAccessToken
    UserInfo(RawlsUserEmail(""), OAuth2BearerToken(aToken.getTokenValue), Option(aToken.getExpirationTime).map(_.getTime).getOrElse(0), RawlsUserSubjectId(""))
  }
}
