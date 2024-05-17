package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class AzureIdentityConfig(managedAppWorkloadClientId: String,
                               managedAppClientId: String,
                               managedAppClientSecret: String,
                               managedAppTenantId: String
)

object AzureIdentityConfig {
  def apply(conf: Config): AzureIdentityConfig =
    AzureIdentityConfig(
      conf.getString("managedAppWorkloadClientId"),
      conf.getString("managedAppClientId"),
      conf.getString("managedAppClientSecret"),
      conf.getString("managedAppTenantId")
    )
}
