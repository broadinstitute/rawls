package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsBillingProjectComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "RawlsBillingProjectComponent" should "save, load and delete" in withEmptyTestDatabase {
    val subjId = RawlsUserSubjectId("17, the most random number")
    val user = RawlsUser(subjId, RawlsUserEmail("email@email.com"))
    val userRef = RawlsUserRef(subjId)

    val ownerSubjId = RawlsUserSubjectId("18, the most random number+1")
    val owner = RawlsUser(ownerSubjId, RawlsUserEmail("email2@email.com"))
    val ownerRef = RawlsUserRef(ownerSubjId)

    val projectName = RawlsBillingProjectName("arbitrary")
    val project = RawlsBillingProject(projectName, generateBillingGroups(projectName, Map(ProjectRoles.Owner -> Set(ownerRef)), Map.empty), "http://cromwell-auth-url.example.com", CreationStatuses.Ready)

    runAndWait(rawlsUserQuery.save(user))
    runAndWait(rawlsUserQuery.save(owner))

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(projectName))
    }

    assertResult(project) {
      runAndWait(rawlsBillingProjectQuery.create(project.projectName, project.cromwellAuthBucketUrl, project.status, Set(ownerRef)))
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

  it should "list projects for users" in withEmptyTestDatabase {
//    val subjId1 = RawlsUserSubjectId("subject ID #1")
//    val subjId2 = RawlsUserSubjectId("This is subject two")
//    val subjId3 = RawlsUserSubjectId("3")
//    val subjId4 = RawlsUserSubjectId("A user with no billing projects")
    val subjId5 = RawlsUserSubjectId("the owner")

//    val email1 = RawlsUserEmail("my1@email.address")
//    val email2 = RawlsUserEmail("my2@email.address")
//    val email3 = RawlsUserEmail("my3@email.address")
//    val email4 = RawlsUserEmail("my4@email.address")
    val email5 = RawlsUserEmail("my5@email.address")

//    val user1 = RawlsUser(subjId1, email1)
//    val user2 = RawlsUser(subjId2, email2)
//    val user3 = RawlsUser(subjId3, email3)
//    val user4 = RawlsUser(subjId4, email4)
    val owner = RawlsUser(subjId5, email5)

//    val userRef1: RawlsUserRef = user1
//    val userRef2: RawlsUserRef = user2
//    val userRef3: RawlsUserRef = user3
//    val userRef4: RawlsUserRef = user4
    val ownerRef: RawlsUserRef = owner

//    val userRecord1 = RawlsUserRecord(subjId1.value, email1.value)
//    val userRecord2 = RawlsUserRecord(subjId2.value, email2.value)
//    val userRecord3 = RawlsUserRecord(subjId3.value, email3.value)
//    val userRecord4 = RawlsUserRecord(subjId4.value, email4.value)
    val ownerRecord = RawlsUserRecord(subjId5.value, email5.value)

//    runAndWait(rawlsUserQuery += userRecord1)
//    runAndWait(rawlsUserQuery += userRecord2)
//    runAndWait(rawlsUserQuery += userRecord3)
//    runAndWait(rawlsUserQuery += userRecord4)
    runAndWait(rawlsUserQuery += ownerRecord)

    val projectName1 = RawlsBillingProjectName("project1")
    val projectName2 = RawlsBillingProjectName("project2")

    val project1 = RawlsBillingProject(projectName1, generateBillingGroups(projectName1, Map(ProjectRoles.Owner -> Set(owner)), Map.empty), "http://cromwell-auth-url.example.com", CreationStatuses.Ready)
    val project2 = RawlsBillingProject(projectName2, generateBillingGroups(projectName2, Map(ProjectRoles.Owner -> Set(owner)), Map.empty), "http://cromwell-auth-url.example.com", CreationStatuses.Ready)

    runAndWait(rawlsBillingProjectQuery.create(project1.projectName, project1.cromwellAuthBucketUrl, project1.status, Set(owner)))
    runAndWait(rawlsBillingProjectQuery.create(project2.projectName, project2.cromwellAuthBucketUrl, project2.status, Set(owner)))

    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, owner, Set(ProjectRoles.User, ProjectRoles.Owner)))
    }
    assert {
      runAndWait(rawlsBillingProjectQuery.hasOneOfProjectRole(projectName1, owner, Set(ProjectRoles.Owner)))
    }

    assertResult(Seq(RawlsBillingProjectMembership(projectName1, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(projectName2, ProjectRoles.Owner, CreationStatuses.Ready))) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(owner))
    }

    val expectedUsersProjects = Map(
      owner -> Seq(projectName1, projectName2))
    expectedUsersProjects should contain theSameElementsAs {
      runAndWait(rawlsBillingProjectQuery.loadAllUsersWithProjects)
    }

    assertResult(Some(project2)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName2))
    }
  }
}
