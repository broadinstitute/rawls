package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsGoogleProject}
import org.scalatest.OptionValues

class GoogleProjectComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with RawlsTestUtils
    with OptionValues
    with RawSqlQuery {

  "RawlsGoogleProjectComponent" should "create" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProject = RawlsGoogleProject(GoogleProjectId("google_project_id"),
                                           billingProject.billingAccount,
                                           Some("message"),
                                           billingProject.projectName
    )
    assertResult(googleProject) {
      runAndWait(rawlsGoogleProjectQuery.create(googleProject))
    }

  }
}
