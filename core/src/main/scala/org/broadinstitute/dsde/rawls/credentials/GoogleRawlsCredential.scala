package org.broadinstitute.dsde.rawls.credentials

import com.google.auth.oauth2.GoogleCredentials

import java.time.Instant

/**
 * Uses Google Auth SDK to create a credential that can be used to authenticate as the Rawls service.
 */
class GoogleRawlsCredential(googleCredentials: GoogleCredentials) extends RawlsCredential {
  override def getExpiresAt: Instant =
    Option(googleCredentials.getAccessToken).map(_.getExpirationTime.toInstant).getOrElse(Instant.now())
  override def getAccessToken: String = googleCredentials.getAccessToken.getTokenValue
  override def refreshToken(): Unit = googleCredentials.refresh()
}
