package org.broadinstitute.dsde.rawls.credentials

case class FakeRawlsCredentials(accessToken: String, expiresIn: Long) extends RawlsCredential {
  override def getExpiresInSeconds: Long = expiresIn
  override def getAccessToken: String = accessToken
  override def refreshToken(): Unit = ()
}
