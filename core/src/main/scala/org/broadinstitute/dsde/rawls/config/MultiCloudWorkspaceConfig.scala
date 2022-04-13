package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import scala.concurrent.duration._
import scala.language.postfixOps


// TODO this data will be pulled from the spend profile service, hardcoding in conf until that svc is ready
final case class MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled: Boolean,
                                           workspaceManager: Option[MultiCloudWorkspaceManagerConfig],
                                           azureConfig: Option[AzureConfig])


final case class MultiCloudWorkspaceManagerConfig(leonardoWsmApplicationId: String, pollTimeout: FiniteDuration)

final case class AzureConfig(spendProfileId: String,
                             azureTenantId: String,
                             azureSubscriptionId: String,
                             azureResourceGroupId: String)


case object MultiCloudWorkspaceConfig {
  def apply[T <: MultiCloudWorkspaceConfig](conf: Config): MultiCloudWorkspaceConfig = {
    val azureConfig: Option[AzureConfig] = conf.getConfigOption("multiCloudWorkspaces.azureConfig") match {
      case Some(azc) => Some(AzureConfig(
        azc.getString("spendProfileId"),
        azc.getString("tenantId"),
        azc.getString("subscriptionId"),
        azc.getString("resourceGroupId")
      ))
      case _ => None
    }

    conf.getConfigOption("multiCloudWorkspaces") match {
      case Some(mc) =>
        new MultiCloudWorkspaceConfig(
          mc.getBoolean("enabled"),
          Some(MultiCloudWorkspaceManagerConfig(
            mc.getString("workspaceManager.leonardoWsmApplicationId"),
            util.toScalaDuration(mc.getDuration("workspaceManager.pollTimeoutSeconds"))
          )),
          azureConfig
      )
      case None =>
        new MultiCloudWorkspaceConfig(false, None, None)
    }
  }
}
