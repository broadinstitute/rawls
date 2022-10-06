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
        |      defaultRegion = "eastus"
        |    },
        |    workspaceManager {
        |      pollTimeoutSeconds = 60 seconds,
        |      leonardoWsmApplicationId = fake_app_id
        |    }
        |}
        |""".stripMargin
    val parsed = ConfigFactory.parseString(enabledConfig)
    val config = MultiCloudWorkspaceConfig.apply(parsed)

    config.multiCloudWorkspacesEnabled shouldBe true
    config.workspaceManager.get.pollTimeout shouldEqual 60.seconds
    config.workspaceManager.get.leonardoWsmApplicationId shouldEqual "fake_app_id"
  }
}
