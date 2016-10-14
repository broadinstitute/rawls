package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "RawlsBillingProjectComponent" should "save, load and delete" in withDefaultTestDatabase {
    val project = testData.projectComponent1
    val projectName = testData.projectComponent1Name

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
    val projectName2 = testData.projectComponent2Name
    val projectName3 = testData.projectComponent3Name

    val project2 = testData.projectComponent2
    val project3 = testData.projectComponent3

    runAndWait(rawlsBillingProjectQuery.create(project2))
    runAndWait(rawlsBillingProjectQuery.create(project3))

    runAndWait(DBIO.sequence(project2.groups.values.map(rawlsGroupQuery.save)))
    runAndWait(DBIO.sequence(project3.groups.values.map(rawlsGroupQuery.save)))

    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userWriter, Set(ProjectRoles.User)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userWriter, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userOwner, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userOwner, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userWriter, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName2, testData.userReader, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName2, ProjectRoles.User, CreationStatuses.Ready))) {
     runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userWriter))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName3, ProjectRoles.User, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userReader))
    }

    assertResult(Seq(RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(projectName2, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(projectName3, ProjectRoles.Owner, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userOwner))
    }

    val expectedUsersProjects = Map(
      testData.userWriter -> Seq(testData.projectComponent1Name, projectName2),
      testData.userReader -> Seq(projectName3),
      testData.userOwner -> Seq(testData.projectComponent1Name, projectName2, projectName3, testData.billingProject.projectName))
    expectedUsersProjects should contain theSameElementsAs {
      runAndWait(rawlsBillingProjectQuery.loadAllUsersWithProjects)
    }

    assertResult(Some(project3)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName3))
    }
  }
}
