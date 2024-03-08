package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class WorkspaceServiceConfigSpec extends AnyFunSpec with Matchers {
  describe("apply") {
    describe("should correctly specify") {
      val workspaceServiceConfig: WorkspaceServiceConfig = WorkspaceServiceConfig.apply(new MultiCloudAppConfigManager)

      it("trackDetailedSubmissionMetrics") {
        workspaceServiceConfig.trackDetailedSubmissionMetrics shouldBe true
      }

      it("workspaceBucketNamePrefix") {
        workspaceServiceConfig.workspaceBucketNamePrefix should fullyMatch regex """\S+"""
      }
    }
  }
}
