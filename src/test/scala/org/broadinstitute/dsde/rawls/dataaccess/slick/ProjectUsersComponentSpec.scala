package org.broadinstitute.dsde.rawls.dataaccess.slick

class ProjectUsersComponentSpec extends TestDriverComponent {

  "ProjectUsersComponent" should "create, load and delete" in {
    val (subjId1, subjId2, subjId3) = ("mySubjectId1", "mySubjectId2", "mySubjectId3")
    val (name1, name2, name3) = ("myName1", "myName2", "myName3")

    val user1 = RawlsUserRecord(subjId1, "arbitrary")
    val user2 = RawlsUserRecord(subjId2, "arbitrary")
    val user3 = RawlsUserRecord(subjId3, "arbitrary")

    val proj1 = RawlsBillingProjectRecord(name1, "arbitrary")
    val proj2 = RawlsBillingProjectRecord(name2, "arbitrary")
    val proj3 = RawlsBillingProjectRecord(name3, "arbitrary")

    import driver.api._
    runAndWait(DBIO.seq(
      saveRawlsUser(user1),
      saveRawlsUser(user2),
      saveRawlsUser(user3),
      saveBillingProject(proj1),
      saveBillingProject(proj2),
      saveBillingProject(proj3)
    ))

    val membership11 = ProjectUsersRecord(subjId1, name1)
    val membership12 = ProjectUsersRecord(subjId1, name2)
    val membership13 = ProjectUsersRecord(subjId1, name3)
    val membership22 = ProjectUsersRecord(subjId2, name2)
    val membership23 = ProjectUsersRecord(subjId2, name3)
    val membership33 = ProjectUsersRecord(subjId3, name3)

    // start empty

    Seq(subjId1, subjId2, subjId3) foreach { case id =>
      assertResult(Seq()) {
        runAndWait(loadProjectUsersBySubjectId(id))
      }
    }

    Seq(name1, name2, name3) foreach { case name =>
      assertResult(Seq()) {
        runAndWait(loadProjectUsersByProjectName(name))
      }
    }

    // insert memberships

    Seq(membership11, membership12, membership13, membership22, membership23, membership33) foreach { case mem =>
      assertResult(mem) {
        runAndWait(saveProjectUsers(mem))
      }
    }

    // query by subject ID

    assertResult(Seq(membership11, membership12, membership13)) {
      runAndWait(loadProjectUsersBySubjectId(subjId1))
    }

    // query by project name

    assertResult(Seq(membership12, membership22)) {
      runAndWait(loadProjectUsersByProjectName(name2))
    }

    // delete by subject ID

    assertResult(2) {   // mem 2/2 and mem 2/3
      runAndWait(deleteProjectUsersBySubjectId(subjId2))
    }

    // delete by project name

    assertResult(2) {   // mem 1/3 and mem 2/3 because mem 2/3 was just deleted
      runAndWait(deleteProjectUsersByProjectName(name3))
    }

    // delete by case class

    assertResult(2) {
      runAndWait(deleteProjectUsers(membership11)) + runAndWait(deleteProjectUsers(membership12))
    }

    // finish empty

    Seq(subjId1, subjId2, subjId3) foreach { case id =>
      assertResult(Seq()) {
        runAndWait(loadProjectUsersBySubjectId(id))
      }
    }

    Seq(name1, name2, name3) foreach { case name =>
      assertResult(Seq()) {
        runAndWait(loadProjectUsersByProjectName(name))
      }
    }
  }
}
