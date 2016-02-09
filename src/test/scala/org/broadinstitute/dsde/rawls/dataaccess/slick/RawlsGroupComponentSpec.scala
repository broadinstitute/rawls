package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._

class RawlsGroupComponentSpec extends TestDriverComponent {

  "RawlsGroupComponent" should "save, load and delete" in withEmptyTestDatabase {
    val groupName = RawlsGroupName("The Avengers")
    val groupEmail = RawlsGroupEmail("avengers@marvel.net")

    val group = RawlsGroup(groupName, groupEmail, Set.empty, Set.empty)
    val groupRef = RawlsGroupRef(groupName)

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupByEmail(groupEmail))
    }

    // implicit translation to RawlsGroupRef
    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(group))
    }

    assertResult(false) {
      runAndWait(rawlsGroupQuery.delete(groupRef))
    }

    // implicit translation to RawlsGroupRef
    assertResult(false) {
      runAndWait(rawlsGroupQuery.delete(group))
    }

    assertResult(group) {
      runAndWait(rawlsGroupQuery.save(group))
    }

    assertResult(Some(group)) {
      runAndWait(rawlsGroupQuery.load(groupRef))
    }

    assertResult(Some(group)) {
      runAndWait(rawlsGroupQuery.loadGroupByEmail(groupEmail))
    }

    assertResult(true) {
      runAndWait(rawlsGroupQuery.delete(groupRef))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupByEmail(groupEmail))
    }
  }

  it should "save, load and delete with users and subgroups" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("12345")
    val subjId2 = RawlsUserSubjectId("6789")
    val subjId3 = RawlsUserSubjectId("not numbers")

    val userRef1 = RawlsUserRef(subjId1)
    val userRef2 = RawlsUserRef(subjId2)
    val userRef3 = RawlsUserRef(subjId3)

    val email1 = RawlsUserEmail("e@mail.com")
    val email2 = RawlsUserEmail("hotmail@yahoo.ru")
    val email3 = RawlsUserEmail("not@valid.email")

    val user1 = RawlsUser(subjId1, email1)
    val user2 = RawlsUser(subjId2, email2)
    val user3 = RawlsUser(subjId3, email3)

    runAndWait(rawlsUserQuery.save(user1))
    runAndWait(rawlsUserQuery.save(user2))
    runAndWait(rawlsUserQuery.save(user3))

    val groupName1 = RawlsGroupName("The Justice League")
    val groupEmail1 = RawlsGroupEmail("justice@dc.org")
    val group1 = RawlsGroup(groupName1, groupEmail1, Set(userRef1, userRef2), Set.empty)
    val groupRef1 = RawlsGroupRef(groupName1)

    val groupName2 = RawlsGroupName("Another Larger Group")
    val groupEmail2 = RawlsGroupEmail("everyone@army.mil")
    val group2 = RawlsGroup(groupName2, groupEmail2, Set(userRef3), Set(groupRef1))
    val groupRef2 = RawlsGroupRef(groupName2)

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef1))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef2))
    }

    assertResult(group1) {
      runAndWait(rawlsGroupQuery.save(group1))
    }

    assertResult(group2) {
      runAndWait(rawlsGroupQuery.save(group2))
    }

    assertResult(Some(group1)) {
      runAndWait(rawlsGroupQuery.load(groupRef1))
    }

    assertResult(Some(group2)) {
      runAndWait(rawlsGroupQuery.load(groupRef2))
    }

    assertResult(Some(Left(user1))) {
      runAndWait(rawlsGroupQuery.loadFromEmail(email1.value))
    }

    assertResult(Some(Right(group2))) {
      runAndWait(rawlsGroupQuery.loadFromEmail(groupEmail2.value))
    }

    assertResult(true) {
      runAndWait(rawlsGroupQuery.delete(groupRef2))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef2))
    }

    assertResult(true) {
      runAndWait(rawlsGroupQuery.delete(groupRef1))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(groupRef1))
    }
  }

  it should "check group membership recursively" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("user1")
    val subjId2 = RawlsUserSubjectId("user2")

    val userRef1 = RawlsUserRef(subjId1)
    val userRef2 = RawlsUserRef(subjId2)

    val email1 = RawlsUserEmail("email1@mail.com")
    val email2 = RawlsUserEmail("email2@mail.net")

    val user1 = RawlsUser(subjId1, email1)
    val user2 = RawlsUser(subjId2, email2)

    runAndWait(rawlsUserQuery.save(user1))
    runAndWait(rawlsUserQuery.save(user2))

    val groupName1 = RawlsGroupName("Group 1")
    val groupEmail1 = RawlsGroupEmail("g1@broad.mit.edu")
    val group1 = RawlsGroup(groupName1, groupEmail1, Set(userRef1), Set.empty)
    val groupRef1 = RawlsGroupRef(groupName1)

    val groupName2 = RawlsGroupName("Group 2")
    val groupEmail2 = RawlsGroupEmail("g2@broadinstitute.org")
    val group2 = RawlsGroup(groupName2, groupEmail2, Set(userRef2), Set(groupRef1))
    val groupRef2 = RawlsGroupRef(groupName2)

    val groupName3 = RawlsGroupName("Group 3")
    val groupEmail3 = RawlsGroupEmail("g3@broad.apple.com")
    val group3 = RawlsGroup(groupName3, groupEmail3, Set.empty, Set(groupRef2))
    val groupRef3 = RawlsGroupRef(groupName3)

    runAndWait(rawlsGroupQuery.save(group1))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))

    assertResult(Some(group1)) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef1, userRef1))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef1, userRef2))
    }

    assertResult(Some(group2)) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef2, userRef1))
    }

    assertResult(Some(group2)) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef2, userRef2))
    }

    assertResult(Some(group3)) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef3, userRef1))
    }

    assertResult(Some(group3)) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef3, userRef2))
    }
  }

  it should "check recursive group membership with cycles" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("user1")
    val subjId2 = RawlsUserSubjectId("user2")

    val email1 = RawlsUserEmail("email1@mail.com")
    val email2 = RawlsUserEmail("email2@mail.net")

    val user1 = RawlsUser(subjId1, email1)
    val user2 = RawlsUser(subjId2, email2)

    runAndWait(rawlsUserQuery.save(user1))
    runAndWait(rawlsUserQuery.save(user2))

    val groupName1 = RawlsGroupName("Group 1")
    val groupEmail1 = RawlsGroupEmail("g1@broad.mit.edu")

    val groupName2 = RawlsGroupName("Group 2")
    val groupEmail2 = RawlsGroupEmail("g2@broadinstitute.org")

    val groupName3 = RawlsGroupName("Group 3")
    val groupEmail3 = RawlsGroupEmail("g3@broad.apple.com")

    val group1NoCycle = RawlsGroup(groupName1, groupEmail1, Set(user1), Set.empty)
    val group2 = RawlsGroup(groupName2, groupEmail2, Set.empty, Set(group1NoCycle))
    val group3 = RawlsGroup(groupName3, groupEmail3, Set.empty, Set(group2))

    runAndWait(rawlsGroupQuery.save(group1NoCycle))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))

    val group1 = group1NoCycle.copy(subGroups = Set(group3))
    runAndWait(rawlsGroupQuery.save(group1))

    Seq(group1, group2, group3) foreach { group =>
      assertResult(Some(group)) {
        runAndWait(rawlsGroupQuery.loadGroupIfMember(group, user1))
      }

      assertResult(None) {
        runAndWait(rawlsGroupQuery.loadGroupIfMember(group, user2))
      }
    }
  }

}
