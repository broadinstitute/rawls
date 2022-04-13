package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{RawlsBillingAccountName, RawlsBillingProjectName}
import org.scalatest.OptionValues

import java.sql.SQLException

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

  it should "create a BillingAccountChange record when the Billing Account is updated with a new non-none value" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val previousBillingAccount = billingProject.billingAccount
    val newBillingAccount = Option(RawlsBillingAccountName("scrooge_mc_ducks_vault"))
    val userId = testData.userOwner.userSubjectId

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
    val billingAccountChange = runAndWait(billingAccountChangeQuery.lastChange(billingProject.projectName))

    billingAccountChange shouldBe defined
    billingAccountChange.value.previousBillingAccount shouldBe previousBillingAccount
    billingAccountChange.value.newBillingAccount shouldBe newBillingAccount
    billingAccountChange.value.userId shouldBe userId
  }

  it should "create a BillingAccountChange record when the Billing Account is set to None" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val previousBillingAccount = billingProject.billingAccount
    val newBillingAccount = None
    val userId = testData.userOwner.userSubjectId

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
    val billingAccountChange = runAndWait(billingAccountChangeQuery.lastChange(billingProject.projectName))

    billingAccountChange shouldBe defined
    billingAccountChange.value.previousBillingAccount shouldBe previousBillingAccount
    billingAccountChange.value.newBillingAccount shouldBe empty
    billingAccountChange.value.userId shouldBe userId
  }

  it should "not create a BillingAccountChange record if the Billing Account is updated with the same value" in withDefaultTestDatabase {
    runAndWait {
      for {
        changeRecordBefore <- billingAccountChangeQuery.lastChange(testData.testProject1Name)
        _ <- rawlsBillingProjectQuery.updateBillingAccount(
          testData.testProject1Name,
          testData.billingProject.billingAccount,
          testData.userOwner.userSubjectId)
        changeRecordAfter <- billingAccountChangeQuery.lastChange(testData.testProject1Name)
      } yield {
        changeRecordBefore shouldBe empty
        changeRecordAfter shouldBe empty
      }
    }
  }

  it should "not throw an exception if changing Billing Account from None to None" in withEmptyTestDatabase {
    runAndWait {
      for {
        _ <- rawlsBillingProjectQuery.create(testData.testProject1.copy(billingAccount = None))
        _ <- rawlsBillingProjectQuery.updateBillingAccount(
          testData.testProject1Name,
          billingAccount = None,
          testData.userOwner.userSubjectId
        )
        _ <- billingAccountChangeQuery.lastChange(testData.testProject1Name)
      } yield ()
    }
  }

  // V2 Billing Projects do not actually need to sync to Google.  We only set Billing Accounts on Google Projects when
  // we create Workspaces, so do not need to audit the Billing Account during Billing Project creation.
  it should "not create a BillingAccountChange record when a Billing Project is first created" in withEmptyTestDatabase {
    runAndWait {
      for {
        _ <- rawlsBillingProjectQuery.create(testData.testProject1)
        billingProject <- rawlsBillingProjectQuery.load(testData.testProject1Name)
        lastChange <- billingAccountChangeQuery.lastChange(testData.testProject1Name)
      } yield {
        billingProject shouldBe defined
        lastChange shouldBe empty
      }
    }
  }

  it should "throw an exception if we try to create a BillingAccountChange record for a Billing Project that does not exist" in withEmptyTestDatabase {
    intercept[SQLException] {
      runAndWait(billingAccountChangeQuery.create(
        RawlsBillingProjectName("kerfluffle"),
        RawlsBillingAccountName("does not matter1").some,
        RawlsBillingAccountName("does not matter2").some,
        testData.userOwner.userSubjectId))
    }
  }
}
