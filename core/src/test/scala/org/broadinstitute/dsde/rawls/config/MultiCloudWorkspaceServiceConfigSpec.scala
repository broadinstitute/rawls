package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class MultiCloudWorkspaceServiceConfigSpec extends AnyFlatSpec with Matchers {
  val testConf: Config = ConfigFactory.load()

  it should "default to not enabled if no config is present" in {
    val config = MultiCloudWorkspaceConfig.apply(ConfigFactory.empty())

    config.multiCloudWorkspacesEnabled shouldBe false
    config.azureConfig shouldBe None
  }

  it should "Parse config when present" in {
    val enabledConfig =
      """
        |multiCloudWorkspaces {
        |    enabled = true
        |    azureConfig {
        |      alphaFeatureGroup = "fake_group",
        |      landingZoneDefinition = "fake_landing_zone_definition",
        |      protectedDataLandingZoneDefinition = "fake_protected_landing_zone_definition"
        |      landingZoneVersion = "fake_landing_zone_version"
        |      landingZoneParameters = {
        |        "FAKE_PARAMETER": "fake_value",
        |        "ANOTHER_FAKE_ONE": "still_not_real"
        |      }
        |      costSavingLandingZoneParameters = {
        |        "FAKE_PARAMETER": "fake_value",
        |        "ANOTHER_FAKE_ONE": "still_not_real"
        |      }
        |    },
        |    workspaceManager {
        |      pollTimeoutSeconds = 60 seconds,
        |      deletionPollTimeoutSeconds = 120 seconds,
        |      leonardoWsmApplicationId = fake_app_id
        |    }
        |}
        |""".stripMargin
    val parsed = ConfigFactory.parseString(enabledConfig)
    val config = MultiCloudWorkspaceConfig.apply(parsed)

    config.multiCloudWorkspacesEnabled shouldBe true
    config.azureConfig.get.landingZoneDefinition shouldBe "fake_landing_zone_definition"
    config.azureConfig.get.landingZoneVersion shouldBe "fake_landing_zone_version"
    config.azureConfig.get.landingZoneParameters shouldBe Map("FAKE_PARAMETER" -> "fake_value",
                                                              "ANOTHER_FAKE_ONE" -> "still_not_real"
    )
    config.azureConfig.get.costSavingLandingZoneParameters shouldBe Map("FAKE_PARAMETER" -> "fake_value",
                                                                        "ANOTHER_FAKE_ONE" -> "still_not_real"
    )
    config.workspaceManager.get.pollTimeout shouldEqual 60.seconds
    config.workspaceManager.get.leonardoWsmApplicationId shouldEqual "fake_app_id"
  }
}
