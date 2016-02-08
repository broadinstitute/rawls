package org.broadinstitute.dsde.rawls.dataaccess.slick

class GroupUsersComponentSpec extends TestDriverComponent {

  "GroupUsersComponent" should "create, load and delete" in {
    val (subjId1, subjId2, subjId3) = ("mySubjectId1", "mySubjectId2", "mySubjectId3")
    val (name1, name2, name3) = ("myName1", "myName2", "myName3")

    val user1 = RawlsUserRecord(subjId1, "arbitrary")
    val user2 = RawlsUserRecord(subjId2, "arbitrary")
    val user3 = RawlsUserRecord(subjId3, "arbitrary")

    val group1 = RawlsGroupRecord(name1, "arbitrary")
    val group2 = RawlsGroupRecord(name2, "arbitrary")
    val group3 = RawlsGroupRecord(name3, "arbitrary")

    import driver.api._
    runAndWait(DBIO.seq(
      saveRawlsUser(user1),
      saveRawlsUser(user2),
      saveRawlsUser(user3),
      saveRawlsGroup(group1),
      saveRawlsGroup(group2),
      saveRawlsGroup(group3)
    ))

    val membership11 = GroupUsersRecord(subjId1, name1)
    val membership12 = GroupUsersRecord(subjId1, name2)
    val membership13 = GroupUsersRecord(subjId1, name3)
    val membership22 = GroupUsersRecord(subjId2, name2)
    val membership23 = GroupUsersRecord(subjId2, name3)
    val membership33 = GroupUsersRecord(subjId3, name3)

    // start empty

    Seq(subjId1, subjId2, subjId3) foreach { case id =>
      assertResult(Seq()) {
        runAndWait(loadGroupUsersBySubjectId(id))
      }
    }

    Seq(name1, name2, name3) foreach { case name =>
      assertResult(Seq()) {
        runAndWait(loadGroupUsersByGroupName(name))
      }
    }

    // insert memberships

    Seq(membership11, membership12, membership13, membership22, membership23, membership33) foreach { case mem =>
      assertResult(mem) {
        runAndWait(saveGroupUsers(mem))
      }
    }

    // query by subject ID

    assertResult(Seq(membership11, membership12, membership13)) {
      runAndWait(loadGroupUsersBySubjectId(subjId1))
    }

    // query by group name

    assertResult(Seq(membership12, membership22)) {
      runAndWait(loadGroupUsersByGroupName(name2))
    }

    // delete by subject ID

    assertResult(2) {
      // mem 2/2 and mem 2/3
      runAndWait(deleteGroupUsersBySubjectId(subjId2))
    }

    // delete by group name

    assertResult(2) {
      // mem 1/3 and mem 2/3 because mem 2/3 was just deleted
      runAndWait(deleteGroupUsersByGroupName(name3))
    }

    // delete by case class

    assertResult(2) {
      runAndWait(deleteGroupUsers(membership11)) + runAndWait(deleteGroupUsers(membership12))
    }

    // finish empty

    Seq(subjId1, subjId2, subjId3) foreach { case id =>
      assertResult(Seq()) {
        runAndWait(loadGroupUsersBySubjectId(id))
      }
    }

    Seq(name1, name2, name3) foreach { case name =>
      assertResult(Seq()) {
        runAndWait(loadGroupUsersByGroupName(name))
      }
    }
  }
}
