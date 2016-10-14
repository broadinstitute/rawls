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
    val projectName1 = testData.projectComponent2Name
    val projectName2 = testData.projectComponent3Name

    val project1 = testData.projectComponent2
    val project2 = testData.projectComponent3

    runAndWait(rawlsBillingProjectQuery.create(project1))
    runAndWait(rawlsBillingProjectQuery.create(project2))

    runAndWait(DBIO.sequence(project1.groups.values.map(rawlsGroupQuery.save)))
    runAndWait(DBIO.sequence(project2.groups.values.map(rawlsGroupQuery.save)))

    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userWriter, Set(ProjectRoles.User)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userWriter, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userOwner, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userOwner, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userWriter, Set(ProjectRoles.Owner)))
    }
    assert {
      !runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, testData.userReader, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName1, ProjectRoles.User, CreationStatuses.Ready))) {
     runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userWriter))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName2, ProjectRoles.User, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userReader))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName1, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(projectName2, ProjectRoles.Owner, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(testData.userOwner))
    }

    val expectedUsersProjects = Map(
      testData -> Seq(projectName1),
      user2 -> Seq(projectName2),
      user3 -> Seq(projectName2),
      user4 -> Seq.empty,
      owner -> Seq(projectName1, projectName2))
    expectedUsersProjects should contain theSameElementsAs {
      runAndWait(rawlsBillingProjectQuery.loadAllUsersWithProjects)
    }

    assertResult(Some(project2)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName2))
    }
  }
}
