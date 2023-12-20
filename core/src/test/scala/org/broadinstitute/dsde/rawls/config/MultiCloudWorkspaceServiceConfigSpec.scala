package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class MultiCloudWorkspaceServiceConfigSpec extends AnyFlatSpec with Matchers {
  val testConf: Config = ConfigFactory.load()

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

    config.azureConfig.landingZoneDefinition shouldBe "fake_landing_zone_definition"
    config.azureConfig.landingZoneVersion shouldBe "fake_landing_zone_version"
    config.azureConfig.landingZoneParameters shouldBe Map("FAKE_PARAMETER" -> "fake_value",
                                                          "ANOTHER_FAKE_ONE" -> "still_not_real"
    )
    config.workspaceManager.pollTimeout shouldEqual 60.seconds
    config.workspaceManager.leonardoWsmApplicationId shouldEqual "fake_app_id"
  }
}
