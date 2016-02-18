package org.broadinstitute.dsde.rawls.dataaccess.slick

class RawlsGroupComponentSpec extends TestDriverComponent {

  "RawlsGroupComponent" should "create, load and delete" in withEmptyTestDatabase {
    val groupName = "arbitrary"
    val group = RawlsGroupRecord(groupName, "rawls@group.example.com")

    assertResult(Seq()) {
      runAndWait(loadRawlsGroup(groupName))
    }

    assertResult(group) {
      runAndWait(saveRawlsGroup(group))
    }

    assertResult(Seq(group)) {
      runAndWait(loadRawlsGroup(groupName))
    }

    assertResult(1) {
      runAndWait(deleteRawlsGroup(groupName))
    }

    assertResult(Seq()) {
      runAndWait(loadRawlsGroup(groupName))
    }
  }
}
