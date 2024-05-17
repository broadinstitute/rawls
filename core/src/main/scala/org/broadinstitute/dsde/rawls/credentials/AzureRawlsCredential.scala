package org.broadinstitute.dsde.rawls.credentials

import com.azure.core.credential.{AccessToken, TokenRequestContext}
import com.azure.identity.DefaultAzureCredentialBuilder
import org.broadinstitute.dsde.rawls.config.AzureIdentityConfig

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference

/**
 * Uses Azure Identity SDK to create a credential that can be used to authenticate as the Rawls service.
 */
class AzureRawlsCredential(config: AzureIdentityConfig) extends RawlsCredential {

  private val azureCredential =
    new DefaultAzureCredentialBuilder()
      .authorityHost(config.azureEnvironment.getActiveDirectoryEndpoint)
      .build()
  private val tokenRequestContext = new TokenRequestContext().addScopes(config.managedIdentityAuthConfig.tokenScope)

  private def fetchAccessToken: AccessToken =
    azureCredential
      .getToken(tokenRequestContext)
      .block(Duration.ofSeconds(config.managedIdentityAuthConfig.tokenAcquisitionTimeout))

  private val token: AtomicReference[AccessToken] = new AtomicReference[AccessToken](fetchAccessToken)

  override def getExpiresAt: Instant = token.get().getExpiresAt.toInstant
  override def getAccessToken: String = token.get().getToken
  override def refreshToken(): Unit = token.set(fetchAccessToken)
}
