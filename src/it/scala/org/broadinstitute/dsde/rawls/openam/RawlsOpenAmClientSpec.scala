package org.broadinstitute.dsde.rawls.openam

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.IntegrationTestConfig
import org.broadinstitute.dsde.vault.common.openam.OpenAMResponse.AuthenticateResponse
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class RawlsOpenAmClientSpec extends WordSpecLike with Matchers with IntegrationTestConfig {
  val timeoutDuration = new FiniteDuration(5, TimeUnit.SECONDS)

  val rawlsOpenAmClient = new RawlsOpenAmClient(new RawlsOpenAmConfig(ConfigFactory.parseFile(new File("/etc/rawls.conf")).getConfig("openam")))

  "RawlsOpenAmClient" should {
    "return a valid token id from authenticate" in {
      val result = Await.result(rawlsOpenAmClient.authenticate, timeoutDuration)
      result shouldNot be (None)
      result.asInstanceOf[AuthenticateResponse].tokenId shouldNot be (null)
      result.asInstanceOf[AuthenticateResponse].successUrl shouldNot be (null)
    }

    // TODO test other stuff
  }
}
