package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.util.ScalaConfig._

import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

final case class MultiCloudWorkspaceConfig(workspaceManager: MultiCloudWorkspaceManagerConfig, azureConfig: AzureConfig)

final case class MultiCloudWorkspaceManagerConfig(leonardoWsmApplicationId: String,
                                                  pollTimeout: FiniteDuration,
                                                  deletionPollTimeout: FiniteDuration
)

final case class AzureConfig(landingZoneDefinition: String,
                             protectedDataLandingZoneDefinition: String,
                             landingZoneVersion: String,
                             landingZoneParameters: Map[String, String],
                             costSavingLandingZoneParameters: Map[String, String],
                             landingZoneAllowAttach: Boolean
)

case object MultiCloudWorkspaceConfig {
  def apply[T <: MultiCloudWorkspaceConfig](conf: Config): MultiCloudWorkspaceConfig = {
    val azc = conf.getConfig("multiCloudWorkspaces.azureConfig")
    val azureConfig =
      AzureConfig(
        azc.getString("landingZoneDefinition"),
        azc.getString("protectedDataLandingZoneDefinition"),
        azc.getString("landingZoneVersion"),
        azc
          .getConfig("landingZoneParameters")
          .entrySet()
          .asScala
          .map { entry =>
            entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String]
          }
          .toMap,
        azc
          .getConfig("costSavingLandingZoneParameters")
          .entrySet()
          .asScala
          .map { entry =>
            entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String]
          }
          .toMap,
        azc.getBooleanOption("landingZoneAllowAttach").getOrElse(false)
      )

    val mc = conf.getConfig("multiCloudWorkspaces")
    new MultiCloudWorkspaceConfig(
      MultiCloudWorkspaceManagerConfig(
        mc.getString("workspaceManager.leonardoWsmApplicationId"),
        util.toScalaDuration(mc.getDuration("workspaceManager.pollTimeoutSeconds")),
        util.toScalaDuration(mc.getDuration("workspaceManager.deletionPollTimeoutSeconds"))
      ),
      azureConfig
    )
  }
}
