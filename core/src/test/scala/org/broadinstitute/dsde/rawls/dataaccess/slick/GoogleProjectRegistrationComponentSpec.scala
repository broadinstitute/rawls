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

  }

  it should "update" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProject = GoogleProjectRegistration(GoogleProjectId("google_project_id"),
                                                  billingProject.billingAccount,
                                                  Some("message"),
                                                  billingProject.projectName
    )
    assertResult(googleProject) {
      runAndWait(googleProjectRegistrationQuery.create(googleProject))
    }
    val billingProject2 = testData.testProject2
    val googleProject2 = GoogleProjectRegistration(GoogleProjectId("google_project_id"),
                                                   billingProject2.billingAccount,
                                                   Some("message"),
                                                   billingProject2.projectName
    )
    assertResult(googleProject2) {
      runAndWait(googleProjectRegistrationQuery.create(googleProject2))
    }

  }
}
