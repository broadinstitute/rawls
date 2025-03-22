package org.broadinstitute.dsde.rawls.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SamModelSpec extends AnyFlatSpec with Matchers {

  "SamGoogleProjectActions" should "have a link action" in {
    SamGoogleProjectActions.link shouldEqual SamResourceAction("link")
  }

  it should "have a readPolicies action" in {
    SamGoogleProjectActions.readPolicies shouldEqual SamResourceAction("read_policies")
  }
}
