package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.Credential
import com.google.auth.oauth2.GoogleCredentials

import scala.util.Try

/**
  * Represents an authenticated user.
  * @param userEmail the user's email address. Resolved to the owner if the request is from a pet.
  * @param accessToken the user's access token. Either a B2C JWT or a Google opaque token.
  * @param accessTokenExpiresIn number of seconds until the access token expires.
  * @param userSubjectId the user id. Either a Google id (numeric) or a B2C id (uuid).
  * @param googleAccessTokenThroughB2C if this is a Google login through B2C, contains the opaque
  *                                    Google access token. Empty otherwise.
  */
case class UserInfo(userEmail: RawlsUserEmail,
                    accessToken: OAuth2BearerToken,
                    accessTokenExpiresIn: Long,
                    userSubjectId: RawlsUserSubjectId,
                    googleAccessTokenThroughB2C: Option[OAuth2BearerToken] = None
) {
  def isB2C: Boolean =
    // B2C ids are uuids, while google ids are numeric
    Try(BigInt(userSubjectId.value)).isFailure
}

object UserInfo {
  def buildFromTokens(credential: Credential): UserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5 * 60) {
      credential.refreshToken()
    }
    val token = OAuth2BearerToken(credential.getAccessToken)
    UserInfo(RawlsUserEmail(""),
             token,
             Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0),
             RawlsUserSubjectId("")
    )
  }

  def buildFromTokens(gCredential: GoogleCredentials): UserInfo = {
    val token = gCredential.refreshAccessToken
    UserInfo(RawlsUserEmail(""),
             OAuth2BearerToken(token.getTokenValue),
             Option(token.getExpirationTime).map(_.getTime).getOrElse(0),
             RawlsUserSubjectId("")
    )
  }
}
