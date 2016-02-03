package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

class RawlsUserComponentSpec extends TestDriverComponent with RawlsUserComponent {

  "RawlsUserComponent" should "create, load and delete" in {
    val userSubjectId = UUID.randomUUID.toString   // subject ID is not a UUID but it's close enough here
    val user = RawlsUserRecord(userSubjectId, "rawls@the-wire.example.com")

    assertResult(Seq()) {
      runAndWait(loadRawlsUser(userSubjectId))
    }

    assertResult(user) {
      runAndWait(saveRawlsUser(user))
    }

    assertResult(Seq(user)) {
      runAndWait(loadRawlsUser(userSubjectId))
    }

    assertResult(1) {
      runAndWait(deleteRawlsUser(userSubjectId))
    }

    assertResult(Seq()) {
      runAndWait(loadRawlsUser(userSubjectId))
    }
  }
}
