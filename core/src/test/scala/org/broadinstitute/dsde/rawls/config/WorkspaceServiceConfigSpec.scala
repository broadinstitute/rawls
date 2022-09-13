package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class WorkspaceServiceConfigSpec extends AnyFunSpec with Matchers {
  // Config for tests loaded from: core/src/test/resources/reference.conf
  val testConf: Config = ConfigFactory.load()

  describe("apply") {
    describe("should correctly specify") {
      val workspaceServiceConfig: WorkspaceServiceConfig = WorkspaceServiceConfig.apply(testConf)

      it("trackDetailedSubmissionMetrics") {
        workspaceServiceConfig.trackDetailedSubmissionMetrics shouldBe true
      }

      it("workspaceBucketNamePrefix") {
        workspaceServiceConfig.workspaceBucketNamePrefix should fullyMatch regex """\S+"""
      }
    }
  }
}
