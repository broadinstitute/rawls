package org.broadinstitute.dsde.rawls.credentials
import java.time.Instant

case class FakeRawlsCredentials(accessToken: String, expiresAt: Instant) extends RawlsCredential {
  override def getExpiresAt: Instant = expiresAt
  override def getAccessToken: String = accessToken
  override def refreshToken(): Unit = ()
}
