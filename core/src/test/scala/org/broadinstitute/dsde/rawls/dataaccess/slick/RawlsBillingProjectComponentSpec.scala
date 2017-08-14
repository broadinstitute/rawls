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

  it should "list projects for users" in withDefaultTestDatabase {
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userWriter, Set(ProjectRoles.User)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userWriter, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userProjectOwner, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userProjectOwner, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userWriter, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(testData.testProject2Name, testData.userReader, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }

    assertResult(Some(testData.testProject3)) {
      runAndWait(rawlsBillingProjectQuery.load(testData.testProject3Name))
    }
  }
}
