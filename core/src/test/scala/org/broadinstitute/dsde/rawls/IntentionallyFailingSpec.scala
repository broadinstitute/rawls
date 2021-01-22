package org.broadinstitute.dsde.rawls

import org.scalatest.freespec.AnyFreeSpec

class IntentionallyFailingSpec extends AnyFreeSpec {
  "Intentionally failing test" - {
    "should fail" in {
      fail("Failing intentionally to check github actions")
    }
  }

}