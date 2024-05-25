package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import com.azure.core.management.AzureEnvironment

case class AzureIdentityConfig(azureEnvironment: AzureEnvironment,
                               managedIdentityAuthConfig: AzureManagedIdentityAuthConfig
)

object AzureIdentityConfig {
  def apply(conf: Config): AzureIdentityConfig =
    AzureIdentityConfig(
      AzureEnvironmentConverter.fromString(conf.getString("azureEnvironment")),
      AzureManagedIdentityAuthConfig(
        conf.getString("managedIdentity.tokenScope"),
        conf.getInt("managedIdentity.tokenAcquisitionTimeout")
      )
    )
}

case class AzureManagedIdentityAuthConfig(
  tokenScope: String,
  tokenAcquisitionTimeout: Int = 30 // in seconds
)

object AzureEnvironmentConverter {
  private val Azure: String = "AZURE"
  private val AzureGov: String = "AZURE_GOV"

  def fromString(s: String): AzureEnvironment = s match {
    case AzureGov => AzureEnvironment.AZURE_US_GOVERNMENT
    case Azure    => AzureEnvironment.AZURE
    case _        => throw new IllegalArgumentException(s"Unknown Azure environment: $s")
  }
}
