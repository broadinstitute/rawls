package org.broadinstitute.dsde.rawls

import org.scalatest.flatspec.AnyFlatSpec

/**
 * Test spec that contains intentional failures, used to exercise
 * Github Actions test-failure modes
 *
 */
class IntentionallyFailingSpec extends AnyFlatSpec {

  behavior of "Intentional test failures"

  it should "fail this test" in {
    fail("this test fails on purpose")
  }


}
