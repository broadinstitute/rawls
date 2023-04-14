package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class LeonardoConfigSpec extends AnyFunSpec with Matchers {
  val testConf: Config =
    ConfigFactory
      .parseMap(Map("wdsType" -> "test-wds-type", "server" -> "test-server-name").asJava)
      .resolve()

  describe("apply") {
    describe("should correctly specify") {
      val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

      it("baseUrl") {
        leonardoConfig.baseUrl shouldBe "test-server-name"
      }

      it("wdsType") {
        leonardoConfig.wdsType shouldBe "test-wds-type"
      }
    }
  }
}
