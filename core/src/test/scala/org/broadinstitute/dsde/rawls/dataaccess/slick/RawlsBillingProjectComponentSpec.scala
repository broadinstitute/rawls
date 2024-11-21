package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{RawlsBillingAccountName, RawlsBillingProjectName, RawlsUserSubjectId}
import org.scalatest.OptionValues

import java.sql.SQLException
import java.time.Instant

class RawlsBillingProjectComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with RawlsTestUtils
    with OptionValues
    with RawSqlQuery {

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
    runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      .getOrElse(fail("project not found"))
      .invalidBillingAccount shouldBe false
    runAndWait(
      rawlsBillingProjectQuery.updateBillingAccountValidity(
        testData.testProject1.billingAccount.getOrElse(fail("missing billing account")),
        true
      )
    )
    runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      .getOrElse(fail("project not found"))
      .invalidBillingAccount shouldBe true
  }

  it should "reset the invalidBillingAccount field when changing the billing account" in withDefaultTestDatabase {
    val project = testData.testProject1
    val newBillingAccount = Option(RawlsBillingAccountName("valid_billing_account"))
    val userId = testData.userOwner.userSubjectId
    runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      .getOrElse(fail("project not found"))
      .invalidBillingAccount shouldBe false
    runAndWait(
      rawlsBillingProjectQuery.updateBillingAccountValidity(
        testData.testProject1.billingAccount.getOrElse(fail("missing billing account")),
        true
      )
    )
    runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      .getOrElse(fail("project not found"))
      .invalidBillingAccount shouldBe true

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(project.projectName, newBillingAccount, userId))
    runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      .getOrElse(fail("project not found"))
      .invalidBillingAccount shouldBe false
  }

  it should "create a BillingAccountChange record when the Billing Account is updated with a new non-none value" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val previousBillingAccount = billingProject.billingAccount
    val newBillingAccount = Option(RawlsBillingAccountName("scrooge_mc_ducks_vault"))
    val userId = testData.userOwner.userSubjectId

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
    val billingAccountChange = runAndWait(BillingAccountChanges.getLastChange(billingProject.projectName))

    billingAccountChange shouldBe defined
    billingAccountChange.value.previousBillingAccount shouldBe previousBillingAccount
    billingAccountChange.value.newBillingAccount shouldBe newBillingAccount
    billingAccountChange.value.userId shouldBe userId
    billingAccountChange.value.status shouldBe BillingAccountChangeStatus.Outstanding
  }

  it should "create a BillingAccountChange record when the Billing Account is set to None" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val previousBillingAccount = billingProject.billingAccount
    val newBillingAccount = None
    val userId = testData.userOwner.userSubjectId

    runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
    val billingAccountChange = runAndWait(BillingAccountChanges.getLastChange(billingProject.projectName))

    billingAccountChange shouldBe defined
    billingAccountChange.value.previousBillingAccount shouldBe previousBillingAccount
    billingAccountChange.value.newBillingAccount shouldBe empty
    billingAccountChange.value.userId shouldBe userId
    billingAccountChange.value.status shouldBe BillingAccountChangeStatus.Outstanding
  }

  List(Option(RawlsBillingAccountName("avalue")), None) foreach { newBillingAccount =>
    it should s"set previous BillingAccountChange records to status=ignored when creating a subsequent change of value $newBillingAccount" in withDefaultTestDatabase {
      import driver.api._

      val billingProject = testData.testProject3
      val userId = testData.userOwner.userSubjectId

      // perform the first update; this inserts a billing account change
      runAndWait(
        rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName,
                                                      Option(RawlsBillingAccountName("first")),
                                                      userId
        )
      )
      // validate the first update
      val firstChange: Option[BillingAccountChange] =
        runAndWait(
          BillingAccountChanges
            .filter(r => r.billingProjectName === billingProject.projectName.value && r.newBillingAccount === "first")
            .result
            .map(_.headOption)
        )
      firstChange shouldBe defined
      firstChange.value.status shouldBe BillingAccountChangeStatus.Outstanding

      // perform the second update; this should set the first update to ignored
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, newBillingAccount, userId))
      val billingAccountChange = runAndWait(BillingAccountChanges.getLastChange(billingProject.projectName))

      billingAccountChange shouldBe defined
      billingAccountChange.value.status shouldBe BillingAccountChangeStatus.Outstanding

      // re-check the first update; it should now be ignored
      val firstChangeAgain: Option[BillingAccountChange] =
        runAndWait(
          BillingAccountChanges
            .filter(r => r.billingProjectName === billingProject.projectName.value && r.newBillingAccount === "first")
            .result
            .map(_.headOption)
        )
      firstChangeAgain shouldBe defined
      firstChangeAgain.value.status shouldBe BillingAccountChangeStatus.Ignored
    }
  }

  it should "fail when the Billing Account is updated with the same value" in
    withDefaultTestDatabase {
      intercept[SQLException] {
        runAndWait {
          for {
            _ <- rawlsBillingProjectQuery.updateBillingAccount(
              testData.testProject1Name,
              testData.billingProject.billingAccount,
              testData.userOwner.userSubjectId
            )
          } yield fail("Should not allow billing accounts to be set to the same value.")
        }
      }
    }

  it should "fail when changing Billing Account from None to None" in
    withEmptyTestDatabase {
      val billingProject = testData.testProject1.copy(billingAccount = None)
      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      intercept[SQLException] {
        runAndWait {
          for {
            _ <- rawlsBillingProjectQuery.updateBillingAccount(
              billingProject.projectName,
              billingAccount = None,
              testData.userOwner.userSubjectId
            )
          } yield fail("Should not allow billing accounts to be set to the same value.")
        }
      }
    }

  // V2 Billing Projects do not actually need to sync to Google.  We only set Billing Accounts on Google Projects when
  // we create Workspaces, so do not need to audit the Billing Account during Billing Project creation.
  it should "not create a BillingAccountChange record when a Billing Project is first created" in withEmptyTestDatabase {
    runAndWait {
      for {
        _ <- rawlsBillingProjectQuery.create(testData.testProject1)
        billingProject <- rawlsBillingProjectQuery.load(testData.testProject1Name)
        lastChange <- BillingAccountChanges.getLastChange(testData.testProject1Name)
      } yield {
        billingProject shouldBe defined
        lastChange shouldBe empty
      }
    }
  }

  it should "throw an exception if we try to create a BillingAccountChange record for a Billing Project that does not exist" in withEmptyTestDatabase {
    intercept[SQLException] {
      runAndWait(
        BillingAccountChanges.create(
          RawlsBillingProjectName("kerfluffle"),
          RawlsBillingAccountName("does not matter1").some,
          RawlsBillingAccountName("does not matter2").some,
          testData.userOwner.userSubjectId
        )
      )
    }
  }

  it should "[CA-1875] not fail to delete a billing project after its billing account has changed" in
    withEmptyTestDatabase {
      runAndWait {
        for {
          _ <- rawlsBillingProjectQuery.create(testData.billingProject)
          _ <- rawlsBillingProjectQuery.updateBillingAccount(
            testData.billingProject.projectName,
            billingAccount = None,
            testData.userOwner.userSubjectId
          )
          _ <- rawlsBillingProjectQuery.delete(testData.billingProject.projectName)
        } yield ()
      }
    }

  it should "set statuses properly in ignoreAllOutstanding" in withDefaultTestDatabase {
    import driver.api._

    // insert some records
    val setupSql = sqlu"""
      insert into BILLING_ACCOUNT_CHANGES
        (BILLING_PROJECT_NAME, USER_ID, PREVIOUS_BILLING_ACCOUNT, NEW_BILLING_ACCOUNT, STATUS)
        values
          (${testData.testProject1Name.value}, 'user', 'oldaccount', 'newaccount', 'failed'),
          (${testData.testProject1Name.value}, 'user', 'oldaccount', 'newaccount', 'synchronized'),
          (${testData.testProject1Name.value}, 'user', 'oldaccount', 'newaccount', 'outstanding'),

          (${testData.testProject2Name.value}, 'user', 'oldaccount', 'newaccount', 'synchronized'),
          (${testData.testProject2Name.value}, 'user', 'oldaccount', 'newaccount', 'synchronized'),
          (${testData.testProject2Name.value}, 'user', 'oldaccount', 'newaccount', 'synchronized'),

          (${testData.testProject3Name.value}, 'user', 'oldaccount', 'newaccount', 'ignored'),
          (${testData.testProject3Name.value}, 'user', 'oldaccount', 'newaccount', 'outstanding'),
          (${testData.testProject3Name.value}, 'user', 'oldaccount', 'newaccount', 'outstanding')
        ;
        """
    runAndWait(setupSql)

    // validate initial setup
    getStatusCountsForProject(testData.testProject1Name) shouldBe Seq(("failed", 1),
                                                                      ("outstanding", 1),
                                                                      ("synchronized", 1)
    )
    getStatusCountsForProject(testData.testProject2Name) shouldBe Seq(("synchronized", 3))
    getStatusCountsForProject(testData.testProject3Name) shouldBe Seq(("ignored", 1), ("outstanding", 2))

    // call ignoreAllOutstanding for testProject1Name and validate
    runAndWait(BillingAccountChanges.ignoreAllOutstanding(testData.testProject1Name))
    getStatusCountsForProject(testData.testProject1Name) shouldBe Seq(("failed", 1),
                                                                      ("ignored", 1),
                                                                      ("synchronized", 1)
    )
    getStatusCountsForProject(testData.testProject2Name) shouldBe Seq(("synchronized", 3))
    getStatusCountsForProject(testData.testProject3Name) shouldBe Seq(("ignored", 1), ("outstanding", 2))

    // call ignoreAllOutstanding for testProject2Name and validate
    runAndWait(BillingAccountChanges.ignoreAllOutstanding(testData.testProject2Name))
    getStatusCountsForProject(testData.testProject1Name) shouldBe Seq(("failed", 1),
                                                                      ("ignored", 1),
                                                                      ("synchronized", 1)
    )
    getStatusCountsForProject(testData.testProject2Name) shouldBe Seq(("synchronized", 3))
    getStatusCountsForProject(testData.testProject3Name) shouldBe Seq(("ignored", 1), ("outstanding", 2))

    // call ignoreAllOutstanding for testProject3Name and validate
    runAndWait(BillingAccountChanges.ignoreAllOutstanding(testData.testProject3Name))
    getStatusCountsForProject(testData.testProject1Name) shouldBe Seq(("failed", 1),
                                                                      ("ignored", 1),
                                                                      ("synchronized", 1)
    )
    getStatusCountsForProject(testData.testProject2Name) shouldBe Seq(("synchronized", 3))
    getStatusCountsForProject(testData.testProject3Name) shouldBe Seq(("ignored", 3))

  }

  private def getStatusCountsForProject(projectName: RawlsBillingProjectName): Seq[(String, Int)] = {
    import driver.api._
    val countSql =
      sql"""
           select STATUS, count(1)
           from BILLING_ACCOUNT_CHANGES
           where BILLING_PROJECT_NAME = ${projectName.value}
           group by STATUS
           order by STATUS
           """.as[(String, Int)]
    runAndWait(countSql)
  }

  it should "latestChangeForProject correctly" is pending
  it should "replacement for readABillingProjectChange should be correct" is pending

  it should "setOutcome/setGoogleSyncTime should also set status" is pending

  "BillingAccountChange" should "be able to load records that need to be sync'd" in withDefaultTestDatabase {
    runAndWait {
      import driver.api._
      for {
        _ <- rawlsBillingProjectQuery.updateBillingAccount(testData.testProject1Name,
                                                           billingAccount = RawlsBillingAccountName("bananas").some,
                                                           testData.userOwner.userSubjectId
        )
        _ <- rawlsBillingProjectQuery.updateBillingAccount(testData.testProject2Name,
                                                           billingAccount = RawlsBillingAccountName("kumquat").some,
                                                           testData.userOwner.userSubjectId
        )
        _ <- rawlsBillingProjectQuery.updateBillingAccount(testData.testProject1Name,
                                                           billingAccount = RawlsBillingAccountName("kumquat").some,
                                                           testData.userOwner.userSubjectId
        )
        changes <- BillingAccountChanges.latestChanges.result

        // We're only concerned with syncing the latest change a user made to the
        // billing project billing account. Right now, we're getting the latest changes
        // in order of ID. We *COULD* get the changes in order of when the first skipped
        // change was made. That's more complicated, so we'll do this for now to keep
        // things simple.
        change1 <- BillingAccountChanges.getLastChange(testData.testProject2Name)
        change2 <- BillingAccountChanges.getLastChange(testData.testProject1Name)
      } yield changes shouldBe List(change1, change2).map(_.value)
    }
  }
}
