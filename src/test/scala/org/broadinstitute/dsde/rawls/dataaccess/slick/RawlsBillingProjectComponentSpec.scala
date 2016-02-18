package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsBillingProjectComponentSpec extends TestDriverComponent {
  import driver.api._

  "RawlsBillingProjectComponent" should "save, load and delete" in withEmptyTestDatabase {
    val subjId = RawlsUserSubjectId("17, the most random number")
    val user = RawlsUser(subjId, RawlsUserEmail("email@email.com"))
    val userRef = RawlsUserRef(subjId)

    val projectName = RawlsBillingProjectName("arbitrary")
    val project = RawlsBillingProject(projectName, Set(userRef), "http://cromwell-auth-url.example.com")

    runAndWait(rawlsUserQuery.save(user))

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(project))
    }

    assertResult(project) {
      runAndWait(rawlsBillingProjectQuery.save(project))
    }

    assertResult(Some(project)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }

    assertResult(true) {
      runAndWait(rawlsBillingProjectQuery.delete(project))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.delete(project))
    }

    assertResult(None) {
      runAndWait(rawlsBillingProjectQuery.load(projectName))
    }
  }

  it should "add and remove users to projects, and list projects for users" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("subject ID #1")
    val subjId2 = RawlsUserSubjectId("This is subject two")
    val subjId3 = RawlsUserSubjectId("3")

    val userRef1 = RawlsUserRef(subjId1)
    val userRef2 = RawlsUserRef(subjId2)
    val userRef3 = RawlsUserRef(subjId3)

    val userRecord1 = RawlsUserRecord(subjId1.value, "my1@email.address")
    val userRecord2 = RawlsUserRecord(subjId2.value, "my2@email.address")
    val userRecord3 = RawlsUserRecord(subjId3.value, "my3@email.address")

    runAndWait(rawlsUserQuery += userRecord1)
    runAndWait(rawlsUserQuery += userRecord2)
    runAndWait(rawlsUserQuery += userRecord3)

    val projectName1 = RawlsBillingProjectName("project1")
    val projectName2 = RawlsBillingProjectName("project2")

    val project1 = RawlsBillingProject(projectName1, Set(userRef1), "http://cromwell-auth-url.example.com")
    val project2 = RawlsBillingProject(projectName2, Set(userRef2, userRef3), "http://cromwell-auth-url.example.com")

    runAndWait(rawlsBillingProjectQuery.save(project1))
    runAndWait(rawlsBillingProjectQuery.save(project2))

    assertResult(Seq(projectName1)) {
     runAndWait(rawlsBillingProjectQuery.listUserProjects(userRef1))
    }

    assertResult(Seq(projectName2)) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(userRef2))
    }

    assertResult(Seq(projectName2)) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(userRef3))
    }

    assertResult(Some(project2)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName2))
    }

    val record = ProjectUsersRecord(subjId1.value, projectName2.value)
    assertResult(record) {
      runAndWait(rawlsBillingProjectQuery.addUserToProject(userRef1, project2))
    }

    val project2PlusUser1 = project2.copy(users = project2.users + userRef1)
    assertResult(Some(project2PlusUser1)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName2))
    }

    assertResult(Seq(projectName1, projectName2)) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(userRef1))
    }

    assertResult(true) {
      runAndWait(rawlsBillingProjectQuery.removeUserFromProject(userRef1, project2))
    }

    assertResult(false) {
      runAndWait(rawlsBillingProjectQuery.removeUserFromProject(userRef1, project2))
    }

    assertResult(Some(project2)) {
      runAndWait(rawlsBillingProjectQuery.load(projectName2))
    }

    assertResult(Seq(projectName1)) {
      runAndWait(rawlsBillingProjectQuery.listUserProjects(userRef1))
    }

  }
}
