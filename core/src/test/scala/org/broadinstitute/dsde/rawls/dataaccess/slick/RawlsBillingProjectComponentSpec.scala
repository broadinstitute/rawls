package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model._

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {
  import driver.api._

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
}
