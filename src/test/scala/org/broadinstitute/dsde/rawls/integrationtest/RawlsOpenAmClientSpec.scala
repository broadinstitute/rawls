package org.broadinstitute.dsde.rawls.integrationtest

import org.broadinstitute.dsde.vault.common.openam.OpenAMResponse.AuthenticateResponse
import scala.concurrent.Await

class RawlsOpenAmClientSpec extends IntegrationTestBase {
  "RawlsOpenAmClient" should "return a valid token id from authenticate" in {
      val result = Await.result(rawlsOpenAmClient.authenticate, timeoutDuration)
      result shouldNot be (None)
      result.asInstanceOf[AuthenticateResponse].tokenId shouldNot be (null)
      result.asInstanceOf[AuthenticateResponse].successUrl shouldNot be (null)
  }
}
