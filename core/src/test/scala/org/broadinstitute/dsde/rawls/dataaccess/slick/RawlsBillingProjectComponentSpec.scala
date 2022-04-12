package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.RawlsBillingAccountName
import org.scalatest.OptionValues

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils with OptionValues {

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
    runAndWait(rawlsBillingProjectQuery.updateBillingAccountValidity(testData.testProject1.billingAccount.getOrElse(fail("missing billing account")), true))
    runAndWait(rawlsBillingProjectQuery.load(project.projectName)).getOrElse(fail("project not found")).invalidBillingAccount shouldBe true
  }

  it should "create a BillingAccountChange record when the Billing Account is updated" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val originalBillingAccount = billingProject.billingAccount
    val newBillingAccount = Option(RawlsBillingAccountName("scrooge_mc_ducks_vault"))
    val userId = testData.userOwner.userSubjectId

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
    val billingAccountChange = runAndWait(billingAccountChangeQuery.lastChange(billingProject.projectName))

    billingAccountChange shouldBe defined
    billingAccountChange.value.originalBillingAccount shouldBe originalBillingAccount
    billingAccountChange.value.newBillingAccount shouldBe newBillingAccount
    billingAccountChange.value.userId shouldBe userId
  }
}
