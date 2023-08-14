package org.broadinstitute.dsde.test.pipeline

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken

case class ProxyAuthToken(userData: UserMetadata, credential: GoogleCredential) extends AuthToken with LazyLogging {
  override def buildCredential(): GoogleCredential = {
    logger.info(s"MockAuthToken.buildCredential() called for user ${userData.email} ...")
    credential.setAccessToken(userData.bearer)
    logger.info("Bearer: " + credential.getAccessToken)
    credential
  }
}
