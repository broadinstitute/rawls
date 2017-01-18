package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsUserComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  "RawlsUserComponent" should "save and load users" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("Subject Number One")
    val email1 = RawlsUserEmail("email@one-direction.net")
    val user1 = RawlsUser(subjId1, email1)

    val subjId2 = RawlsUserSubjectId("A Second Subject")
    val email2 = RawlsUserEmail("two.emails@are.better.than.one")
    val user2 = RawlsUser(subjId2, email2)

    assertResult(Seq.empty) {
      runAndWait(rawlsUserQuery.loadAllUsers())
    }

    assertResult(user1) {
      runAndWait(rawlsUserQuery.save(user1))
    }

    // second save is update, not duplicate
    assertResult(user1) {
      runAndWait(rawlsUserQuery.save(user1))
    }

    assertResult(user2) {
      runAndWait(rawlsUserQuery.save(user2))
    }

    assertResult(Seq(user1, user2)) {
      runAndWait(rawlsUserQuery.loadAllUsers())
    }
  }

  it should "load users by reference and by email" in withEmptyTestDatabase {
    val subjId = RawlsUserSubjectId("Subject")
    val email = RawlsUserEmail("email@hotmail.com")
    val user = RawlsUser(subjId, email)

    assertResult(Seq.empty) {
      runAndWait(rawlsUserQuery.loadAllUsers())
    }

    assertResult(user) {
      runAndWait(rawlsUserQuery.save(user))
    }

    assertResult(Some(user)) {
      runAndWait(rawlsUserQuery.load(RawlsUserRef(subjId)))
    }

    // implicit translation to RawlsUserRef
    assertResult(Some(user)) {
      runAndWait(rawlsUserQuery.load(user))
    }

    assertResult(Some(user)) {
      runAndWait(rawlsUserQuery.loadUserByEmail(user.userEmail))
    }
  }

  it should "count users" in withEmptyTestDatabase {
    assertResult(SingleStatistic(0)) {
      runAndWait(rawlsUserQuery.countUsers())
    }

    val subjId = RawlsUserSubjectId("Subject")
    val email = RawlsUserEmail("email@hotmail.com")
    val user = RawlsUser(subjId, email)

    assertResult(user) {
      runAndWait(rawlsUserQuery.save(user))
    }

    assertResult(SingleStatistic(1)) {
      runAndWait(rawlsUserQuery.countUsers())
    }
  }

  it should "list groups user belongs to" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("Subject Number One")
    val email1 = RawlsUserEmail("email@one-direction.net")
    val user1 = RawlsUser(subjId1, email1)

    val subjId2 = RawlsUserSubjectId("A Second Subject")
    val email2 = RawlsUserEmail("two.emails@are.better.than.one")
    val user2 = RawlsUser(subjId2, email2)


    //    val testUser2 = testUser.copy(userSubjectId = RawlsUserSubjectId("dummy-ID2"), userEmail = RawlsUserEmail("dummy-email2@example.com"))
    val group1 = makeRawlsGroup("Group One", Set(user1))
    val group2 = makeRawlsGroup("Group Two", Set(user2)).copy(subGroups = Set(group1))
    val group3 = makeRawlsGroup("Group Three", Set.empty).copy(subGroups = Set(group2))
    val group4 = makeRawlsGroup("Group Four", Set.empty).copy(subGroups = Set(group1))
    val group5 = makeRawlsGroup("Group Five", Set.empty).copy(subGroups = Set(group3))

    runAndWait(rawlsUserQuery.save(user1))
    runAndWait(rawlsUserQuery.save(user2))
    runAndWait(rawlsGroupQuery.save(group1))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group4))
    runAndWait(rawlsGroupQuery.save(group5))

    assertResult(Set(RawlsGroupRef(RawlsGroupName("Group Two")),RawlsGroupRef(RawlsGroupName("Group Two")),RawlsGroupRef(RawlsGroupName("Group Three")),RawlsGroupRef(RawlsGroupName("Group Five")))) {
      runAndWait(rawlsGroupQuery.listGroupsForUser(user2))
    }
  }


}
