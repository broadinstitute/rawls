package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "RawlsBillingProjectComponent" should "save, load and delete" in withDefaultTestDatabase {
    val project = testData.testProject1
    val projectName = testData.testProject1Name

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(projectName))
    }

    assertResult(project) {
      runAndWait(rawlsBillingProjectQuery.create(project))
    }

    assertResult(Some(project)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }

    assertResult(true) {
      runAndWait(rawlsBillingProjectQuery.delete(projectName))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(projectName))
    }

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }
  }

  it should "list projects for users" in withDefaultTestDatabase {
    val project2Name = testData.testProject2Name
    val project3Name = testData.testProject3Name

    val project2 = testData.testProject2
    val project3 = testData.testProject3

    runAndWait(rawlsBillingProjectQuery.create(project2))
    runAndWait(rawlsBillingProjectQuery.create(project3))

    runAndWait(DBIO.sequence(project2.groups.values.map(rawlsGroupQuery.save)))
    runAndWait(DBIO.sequence(project3.groups.values.map(rawlsGroupQuery.save)))

    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userWriter, Set(ProjectRoles.User)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userWriter, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userOwner, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userOwner, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userWriter, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(project2Name, testData.userReader, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }

    assertResult(Seq(RawlsBillingProjectMembership(project2Name, ProjectRoles.User, CreationStatuses.Ready))) {
     runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userWriter))
    }

    assertResult(Seq(RawlsBillingProjectMembership(project3Name, ProjectRoles.User, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userReader))
    }

    assertResult(Seq(RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(project2Name, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(project3Name, ProjectRoles.Owner, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userOwner))
    }

    val expectedUsersProjects = Map(
      testData.userWriter -> Seq(project2Name),
      testData.userReader -> Seq(project3Name),
      testData.userOwner -> Seq(testData.billingProject.projectName, project2Name, project3Name))
    expectedUsersProjects should contain theSameElementsAs  {
      runAndWait(rawlsBillingProjectQuery.loadAllUsersWithProjects)
    }

    assertResult(Some(project3)) {
      runAndWait(rawlsBillingProjectQuery.load(project3Name))
    }
  }
}
