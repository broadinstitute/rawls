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

  it should "save concurrently" in withEmptyTestDatabase {
    val subjId = RawlsUserSubjectId("SubjectID")
    val email = RawlsUserEmail("emails@hotmail.com")
    val user = RawlsUser(subjId, email)

    def userGenerator(i: Int) = {
      rawlsUserQuery.save(user.copy(userEmail = RawlsUserEmail(s"abby.testerson.$i@broad.example.com")))
    }

    val count = 100
    runMultipleAndWait(count)(userGenerator)

    assert {
      runAndWait(rawlsUserQuery.load(user)).nonEmpty
    }
  }

}
