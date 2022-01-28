package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.scalatest.OptionValues

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {

  "RawlsBillingProjectComponent" should "save, load and delete" in withDefaultTestDatabase {
    // note that create is called in test data save
    val project = testData.testProject1
    assertResult(Some(project)) {
      runAndWait(rawlsBillingProjectQuery.load(project.projectName))
    }

    assertResult(true) {
      runAndWait(rawlsBillingProjectQuery.delete(project.projectName))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(project.projectName))
    }

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(project.projectName))
    }
  }

  it should "be able to update the invalidBillingAccount field" in withDefaultTestDatabase {
    val project = testData.testProject1

    runAndWait(rawlsBillingProjectQuery.load(project.projectName)).getOrElse(fail("project not found")).invalidBillingAccount shouldBe false

    runAndWait(rawlsBillingProjectQuery.updateBillingAccountValidity(testData.testProject1.billingAccount.getOrElse(fail("missing billing account")), false))

    runAndWait(rawlsBillingProjectQuery.load(project.projectName)).getOrElse(fail("project not found")).invalidBillingAccount shouldBe true
  }
}
