package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model._
import java.sql.SQLException

class RawlsGroupComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  "RawlsGroupComponent" should "save, load and delete" in withEmptyTestDatabase {
    val groupName = RawlsGroupName("The-Avengers")
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

    // a second save is an update, not a duplication
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

    // a second delete will fail
    assertResult(false) {
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

    runAndWait(rawlsUserQuery.createUser(user1))
    runAndWait(rawlsUserQuery.createUser(user2))
    runAndWait(rawlsUserQuery.createUser(user3))

    val groupName1 = RawlsGroupName("The-Justice-League")
    val groupEmail1 = RawlsGroupEmail("justice@dc.org")
    val group1 = RawlsGroup(groupName1, groupEmail1, Set(userRef1, userRef2), Set.empty)
    val groupRef1 = RawlsGroupRef(groupName1)

    val groupName2 = RawlsGroupName("Another-Larger-Group")
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

    runAndWait(rawlsUserQuery.createUser(user1))
    runAndWait(rawlsUserQuery.createUser(user2))

    val groupName1 = RawlsGroupName("Group-1")
    val groupEmail1 = RawlsGroupEmail("g1@broad.mit.edu")
    val group1 = RawlsGroup(groupName1, groupEmail1, Set(userRef1), Set.empty)
    val groupRef1 = RawlsGroupRef(groupName1)

    val groupName2 = RawlsGroupName("Group-2")
    val groupEmail2 = RawlsGroupEmail("g2@broadinstitute.org")
    val group2 = RawlsGroup(groupName2, groupEmail2, Set(userRef2), Set(groupRef1))
    val groupRef2 = RawlsGroupRef(groupName2)

    val groupName3 = RawlsGroupName("Group-3")
    val groupEmail3 = RawlsGroupEmail("g3@broad.apple.com")
    val group3 = RawlsGroup(groupName3, groupEmail3, Set.empty, Set(groupRef2))
    val groupRef3 = RawlsGroupRef(groupName3)

    runAndWait(rawlsGroupQuery.save(group1))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))

    assertResult(Set(group1, group2, group3)) {
      runAndWait(rawlsGroupQuery.loadGroupsRecursive(Set(groupRef3)))
    }

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

    runAndWait(rawlsGroupQuery.removeGroupMember(groupName1, userRef1.userSubjectId))

    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef1, userRef1))
    }
    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef2, userRef1))
    }
    assertResult(None) {
      runAndWait(rawlsGroupQuery.loadGroupIfMember(groupRef3, userRef1))
    }
  }

  it should "check recursive group membership with cycles" in withEmptyTestDatabase {
    val subjId1 = RawlsUserSubjectId("user1")
    val subjId2 = RawlsUserSubjectId("user2")

    val email1 = RawlsUserEmail("email1@mail.com")
    val email2 = RawlsUserEmail("email2@mail.net")

    val user1 = RawlsUser(subjId1, email1)
    val user2 = RawlsUser(subjId2, email2)

    runAndWait(rawlsUserQuery.createUser(user1))
    runAndWait(rawlsUserQuery.createUser(user2))

    val groupName1 = RawlsGroupName("Group-1")
    val groupEmail1 = RawlsGroupEmail("g1@broad.mit.edu")

    val groupName2 = RawlsGroupName("Group-2")
    val groupEmail2 = RawlsGroupEmail("g2@broadinstitute.org")

    val groupName3 = RawlsGroupName("Group-3")
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
      assertResult(Set(group1, group2, group3)) {
        runAndWait(rawlsGroupQuery.loadGroupsRecursive(Set(group)))
      }

      assertResult(Some(group)) {
        runAndWait(rawlsGroupQuery.loadGroupIfMember(group, user1))
      }

      assertResult(Set(group1, group2, group3).map(RawlsGroup.toRef)) {
        runAndWait(rawlsGroupQuery.listGroupsForUser(user1))
      }

      assertResult(None) {
        runAndWait(rawlsGroupQuery.loadGroupIfMember(group, user2))
      }
    }
  }

  val userSubjId = "dummy-ID"
  val userEmailDummy = "dummy-email@example.com"
  val testUser = RawlsUser(RawlsUserSubjectId(userSubjId), RawlsUserEmail(userEmailDummy))

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  it should "save a User to multiple Groups but not save two copies of the same User" in withEmptyTestDatabase {

    val group1 = makeRawlsGroup("Group-1-For-User", Set(testUser))
    val group2 = makeRawlsGroup("Group-2-For-User", Set(testUser))

    runAndWait(rawlsUserQuery.createUser(testUser))
    runAndWait(rawlsGroupQuery.save(group1))
    runAndWait(rawlsGroupQuery.save(group2))

    assertResult(Seq(testUser)) {
      runAndWait(rawlsUserQuery.loadAllUsers())
    }

    assertResult(Some(group1)) {
      runAndWait(rawlsGroupQuery.load(group1))
    }

    assertResult(Some(group2)) {
      runAndWait(rawlsGroupQuery.load(group2))
    }
  }

  it should "not save a new Group with missing users" in withEmptyTestDatabase {
    val user1 = userFromId("subjectId1")
    val user2 = userFromId("subjectId2")
    val group = makeRawlsGroup("Two-User-Group", Set(user1, user2))

    intercept[SQLException] {
      // note that the users have not first been saved
      runAndWait(rawlsGroupQuery.save(group))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(group))
    }
  }

  it should "not save a new Group with missing groups" in withEmptyTestDatabase {
    val group1 = makeRawlsGroup("Group-One", Set.empty)
    val group2 = makeRawlsGroup("Group-Two", Set.empty).copy(subGroups = Set(group1))

    intercept[SQLException] {
      // note that group1 has not first been saved
      runAndWait(rawlsGroupQuery.save(group2))
    }

    assertResult(None) {
      runAndWait(rawlsGroupQuery.load(group2))
    }
  }

  it should "flatten group membership" in withEmptyTestDatabase {
    val testUser2 = testUser.copy(userSubjectId = RawlsUserSubjectId("dummy-ID2"), userEmail = RawlsUserEmail("dummy-email2@example.com"))
    val group1 = makeRawlsGroup("Group-One", Set(testUser))
    val group2 = makeRawlsGroup("Group-Two", Set(testUser2)).copy(subGroups = Set(group1))
    val group3 = makeRawlsGroup("Group-Three", Set.empty).copy(subGroups = Set(group2))
    val group4 = makeRawlsGroup("Group-Four", Set.empty).copy(subGroups = Set(group3))
    val group5 = makeRawlsGroup("Group-Five", Set.empty).copy(subGroups = Set(group3))

    runAndWait(rawlsUserQuery.createUser(testUser))
    runAndWait(rawlsUserQuery.createUser(testUser2))
    runAndWait(rawlsGroupQuery.save(group1))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group4))
    runAndWait(rawlsGroupQuery.save(group5))

    assertResult(Set(testUser, testUser2).map(u => RawlsUserRef(u.userSubjectId))) {
      runAndWait(rawlsGroupQuery.flattenGroupMembership(group5))
    }

  }

  it should "return the intersection of a realm and a group" in withEmptyTestDatabase {
    val obama = RawlsUser(RawlsUserSubjectId("obama"), RawlsUserEmail("obama@whitehouse.gov"))
    val trump = RawlsUser(RawlsUserSubjectId("donald"), RawlsUserEmail("thedonald@donaldjtrump.com"))
    val arod = RawlsUser(RawlsUserSubjectId("arod"), RawlsUserEmail("alexrodriguez@mlb.com"))
    val clinton = RawlsUser(RawlsUserSubjectId("hillary"), RawlsUserEmail("hillary@maybewhitehouse.gov"))
    val bernie = RawlsUser(RawlsUserSubjectId("bernie"), RawlsUserEmail("bernie@vermont.gov"))
    val group = makeRawlsGroup("Good", Set(obama, bernie))
    val group2 = makeRawlsGroup("Evil", Set(trump, arod))
    val group3 = makeRawlsGroup("Other", Set(clinton))
    val group4 = makeRawlsGroup("Good-And-Evil", Set.empty).copy(subGroups =  Set(group, group2, group3))
    val realm = makeRawlsGroup("Politicos", Set(obama, trump, bernie)).copy(subGroups = Set(group3))

    runAndWait(rawlsUserQuery.createUser(obama))
    runAndWait(rawlsUserQuery.createUser(trump))
    runAndWait(rawlsUserQuery.createUser(arod))
    runAndWait(rawlsUserQuery.createUser(clinton))
    runAndWait(rawlsUserQuery.createUser(bernie))
    runAndWait(rawlsGroupQuery.save(group))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group4))
    runAndWait(rawlsGroupQuery.save(realm))

    assertResult(Set(RawlsUserRef(obama.userSubjectId), RawlsUserRef(trump.userSubjectId), RawlsUserRef(clinton.userSubjectId), RawlsUserRef(bernie.userSubjectId))) {
      runAndWait(rawlsGroupQuery.intersectGroupMembership(Set(realm, group4)))
    }
  }


}