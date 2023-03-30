package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

final case class MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled: Boolean,
                                           workspaceManager: Option[MultiCloudWorkspaceManagerConfig],
                                           azureConfig: Option[AzureConfig],
)

final case class MultiCloudWorkspaceManagerConfig(leonardoWsmApplicationId: String, pollTimeout: FiniteDuration)

final case class AzureConfig(landingZoneDefinition: String,
                             landingZoneVersion: String,
                             landingZoneParameters: Map[String, String]
)

case object MultiCloudWorkspaceConfig {
  def apply[T <: MultiCloudWorkspaceConfig](conf: Config): MultiCloudWorkspaceConfig = {
    val azureConfig: Option[AzureConfig] = conf.getConfigOption("multiCloudWorkspaces.azureConfig") match {
      case Some(azc) =>
        Some(
          AzureConfig(
            azc.getString("landingZoneDefinition"),
            azc.getString("landingZoneVersion"),
            azc
              .getConfig("landingZoneParameters")
              .entrySet()
              .asScala
              .map { entry =>
                entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String]
              }
              .toMap
          ),
      )
      case _ => None
    }

    conf.getConfigOption("multiCloudWorkspaces") match {
      case Some(mc) =>
        new MultiCloudWorkspaceConfig(
          mc.getBoolean("enabled"),
          Some(
            MultiCloudWorkspaceManagerConfig(
              mc.getString("workspaceManager.leonardoWsmApplicationId"),
              util.toScalaDuration(mc.getDuration("workspaceManager.pollTimeoutSeconds"))
            )
          ),
          azureConfig
        )
      case None =>
        new MultiCloudWorkspaceConfig(false, None, None, None)
    }
  }
}
