package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.config.LeonardoConfig

class LeonardoConfigSpec extends AnyFunSpec with Matchers {
  // Config for tests loaded from: core/src/test/resources/reference.conf
  val testConf: Config = ConfigFactory.load()

  describe("apply") {
    describe("should correctly specify") {
      val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

      it("baseUrl") {
        leonardoConfig.baseUrl shouldBe "https://leonardo.dsde-dev.broadinstitute.org"
      }
    }
  }
}
