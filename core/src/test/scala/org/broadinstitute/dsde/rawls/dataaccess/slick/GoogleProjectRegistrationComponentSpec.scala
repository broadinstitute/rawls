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

}
