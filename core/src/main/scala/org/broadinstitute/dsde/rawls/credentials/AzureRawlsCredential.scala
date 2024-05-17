package org.broadinstitute.dsde.rawls.credentials

import com.azure.core.credential.{AccessToken, TokenRequestContext}
import com.azure.identity.{
  ChainedTokenCredentialBuilder,
  ClientSecretCredentialBuilder,
  ManagedIdentityCredentialBuilder
}
import org.broadinstitute.dsde.rawls.config.AzureIdentityConfig

import java.util.concurrent.atomic.AtomicReference

class AzureRawlsCredential(config: AzureIdentityConfig) extends RawlsCredential {

  private val azureCredential = {
    val managedIdentityCredential = new ManagedIdentityCredentialBuilder()
      .clientId(config.managedAppWorkloadClientId)
      .build

    val servicePrincipalCredential = new ClientSecretCredentialBuilder()
      .clientId(config.managedAppClientId)
      .clientSecret(config.managedAppClientSecret)
      .tenantId(config.managedAppTenantId)
      .build

    // When an access token is requested, the chain will try each
    // credential in order, stopping when one provides a token
    //
    // For Managed Identity auth, SAM must be deployed to an Azure service
    // other platforms will fall through to Service Principal auth
    new ChainedTokenCredentialBuilder()
      .addLast(managedIdentityCredential)
      .addLast(servicePrincipalCredential)
      .build
  }

  private def fetchAccessToken: AccessToken =
    azureCredential.getTokenSync(new TokenRequestContext())

  private val token: AtomicReference[AccessToken] = new AtomicReference[AccessToken](fetchAccessToken)

  override def getExpiresInSeconds: Long = token.get().getExpiresAt.toEpochSecond - System.currentTimeMillis() / 1000
  override def getAccessToken: String = token.get().getToken
  override def refreshToken(): Unit = token.set(fetchAccessToken)
}
