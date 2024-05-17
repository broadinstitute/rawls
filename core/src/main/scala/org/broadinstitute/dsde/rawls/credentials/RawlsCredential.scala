package org.broadinstitute.dsde.rawls.credentials

import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.rawls.config.{AzureIdentityConfig, RawlsConfigManager}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.util.Using

trait RawlsCredential {
  def getExpiresInSeconds: Long
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
