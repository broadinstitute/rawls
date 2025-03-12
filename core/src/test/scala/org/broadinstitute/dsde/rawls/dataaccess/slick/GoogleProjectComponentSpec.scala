package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.RawlsGoogleProject
import org.scalatest.OptionValues

class GoogleProjectComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with RawlsTestUtils
    with OptionValues
    with RawSqlQuery {

  "RawlsGoogleProjectComponent" should "create" in withDefaultTestDatabase {
    val billingProject = testData.testProject1
    val googleProject = RawlsGoogleProject("google_project_id",
                                           billingProject.billingAccount.map(_.toString),
                                           Some("message"),
                                           billingProject.projectName
    )
    assertResult(googleProject) {
      runAndWait(rawlsGoogleProjectQuery.create(googleProject))
    }

  }
}
