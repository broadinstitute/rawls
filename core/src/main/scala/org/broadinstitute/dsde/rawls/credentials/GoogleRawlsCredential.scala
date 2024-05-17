package org.broadinstitute.dsde.rawls.credentials

import com.google.auth.oauth2.GoogleCredentials

class GoogleRawlsCredential(googleCredentials: GoogleCredentials) extends RawlsCredential {
  override def getExpiresInSeconds: Long =
    Option(googleCredentials.getAccessToken).map(_.getExpirationTime.getTime).getOrElse(0L)
  override def getAccessToken: String = googleCredentials.getAccessToken.getTokenValue
  override def refreshToken(): Unit = googleCredentials.refresh()
}
