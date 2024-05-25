package org.broadinstitute.dsde.rawls.credentials

import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.rawls.config.{AzureIdentityConfig, RawlsConfigManager}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.util.Using

/**
 * Represents a credential that can be used to authenticate as the Rawls service.
 */
trait RawlsCredential {
  def getExpiresAt: Instant
  def getAccessToken: String
  def refreshToken(): Unit
}

object RawlsCredential {
  def getCredential(configManager: RawlsConfigManager): RawlsCredential =
    configManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val saCredential = Using(jsonFileSource) { jsonFileSource =>
          ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(jsonFileSource.mkString.getBytes(StandardCharsets.UTF_8))
          )
        }
        new GoogleRawlsCredential(saCredential.get.createScoped("openid", "email", "profile"))
      case _ => new AzureRawlsCredential(AzureIdentityConfig(configManager.conf.getConfig("azureIdentity")))
    }
}
