package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectRegistration}
import org.scalatest.OptionValues

class GoogleProjectRegistrationComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with RawlsTestUtils
    with OptionValues
    with RawSqlQuery {

  "RawlsGoogleProjectComponent" should "create" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProject = GoogleProjectRegistration(GoogleProjectId("google_project_id"),
                                                  billingProject.billingAccount,
                                                  Some("message"),
                                                  billingProject.projectName
    )
    assertResult(googleProject) {
      runAndWait(googleProjectRegistrationQuery.create(googleProject))
    }

    // Make sure it exists
    assertResult(Some(googleProject)) {
      runAndWait(googleProjectRegistrationQuery.findById(googleProject.googleProjectId))
    }

  }

  "RawlsGoogleProjectComponent" should "delete" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProject = GoogleProjectRegistration(GoogleProjectId("google_project_id"),
                                                  billingProject.billingAccount,
                                                  Some("message"),
                                                  billingProject.projectName
    )
    // Create
    runAndWait(googleProjectRegistrationQuery.create(googleProject))

    // Verify it was created
    assertResult(Some(googleProject)) {
      runAndWait(googleProjectRegistrationQuery.findById(googleProject.googleProjectId))
    }

    // Delete
    runAndWait(googleProjectRegistrationQuery.delete(googleProject.googleProjectId))

    // Verify it's gone
    assertResult(None) {
      runAndWait(googleProjectRegistrationQuery.findById(googleProject.googleProjectId))
    }

  }

  it should "findByIdsAndBillingProject" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProjectRegistrations = Seq(
      GoogleProjectRegistration(GoogleProjectId("project1"),
                                billingProject.billingAccount,
                                Some("message1"),
                                billingProject.projectName
      ),
      GoogleProjectRegistration(GoogleProjectId("project2"),
                                billingProject.billingAccount,
                                Some("message2"),
                                billingProject.projectName
      ),
      GoogleProjectRegistration(GoogleProjectId("project3"),
                                billingProject.billingAccount,
                                Some("message3"),
                                billingProject.projectName
      )
    )

    googleProjectRegistrations.foreach { googleProject =>
      runAndWait(googleProjectRegistrationQuery.create(googleProject))
    }

    val result = runAndWait(
      googleProjectRegistrationQuery.findByIdsAndBillingProject(Set(GoogleProjectId("project1"),
                                                                    GoogleProjectId("project2")
                                                                ),
                                                                None,
                                                                10,
                                                                0
      )
    )

    result should have size 2
    result.map(_.googleProjectId.value) should contain only ("project1", "project2")
  }

  it should "findByIdsAndBillingProject with pagination" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProjectRegistrations = Seq(
      GoogleProjectRegistration(GoogleProjectId("project1"),
                                billingProject.billingAccount,
                                Some("message1"),
                                billingProject.projectName
      ),
      GoogleProjectRegistration(GoogleProjectId("project2"),
                                billingProject.billingAccount,
                                Some("message2"),
                                billingProject.projectName
      ),
      GoogleProjectRegistration(GoogleProjectId("project3"),
                                billingProject.billingAccount,
                                Some("message3"),
                                billingProject.projectName
      ),
      GoogleProjectRegistration(GoogleProjectId("project4"),
                                billingProject.billingAccount,
                                Some("message4"),
                                billingProject.projectName
      )
    )

    googleProjectRegistrations.foreach { googleProject =>
      runAndWait(googleProjectRegistrationQuery.create(googleProject))
    }

    // Test first page
    val firstPageResult = runAndWait(
      googleProjectRegistrationQuery.findByIdsAndBillingProject(Set(GoogleProjectId("project1"),
                                                                    GoogleProjectId("project2"),
                                                                    GoogleProjectId("project3"),
                                                                    GoogleProjectId("project4")
                                                                ),
                                                                None,
                                                                2,
                                                                0
      )
    )

    firstPageResult should have size 2
    firstPageResult.map(_.googleProjectId.value) should contain only ("project1", "project2")

    // Test second page
    val secondPageResult = runAndWait(
      googleProjectRegistrationQuery.findByIdsAndBillingProject(Set(GoogleProjectId("project1"),
                                                                    GoogleProjectId("project2"),
                                                                    GoogleProjectId("project3"),
                                                                    GoogleProjectId("project4")
                                                                ),
                                                                None,
                                                                2,
                                                                2
      )
    )

    secondPageResult should have size 2
    secondPageResult.map(_.googleProjectId.value) should contain only ("project3", "project4")
  }
}
