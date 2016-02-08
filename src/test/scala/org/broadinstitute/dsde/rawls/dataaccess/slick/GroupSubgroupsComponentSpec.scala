package org.broadinstitute.dsde.rawls.dataaccess.slick

class GroupSubgroupsComponentSpec extends TestDriverComponent {

  "GroupSubgroupsComponent" should "create, load and delete" in {
    val (parent1, parent2, parent3) = ("parent1", "parent2", "parent3")
    val (child1, child2, child3) = ("child1", "child2", "child3")

    val p1 = RawlsGroupRecord(parent1, "arbitrary")
    val p2 = RawlsGroupRecord(parent2, "arbitrary")
    val p3 = RawlsGroupRecord(parent3, "arbitrary")

    val c1 = RawlsGroupRecord(child1, "arbitrary")
    val c2 = RawlsGroupRecord(child2, "arbitrary")
    val c3 = RawlsGroupRecord(child3, "arbitrary")

    Seq(p1, p2, p3, c1, c2, c3) foreach { group =>
      runAndWait(saveRawlsGroup(group))
    }

    val membership11 = GroupSubgroupsRecord(parent1, child1)
    val membership12 = GroupSubgroupsRecord(parent1, child2)
    val membership13 = GroupSubgroupsRecord(parent1, child3)
    val membership22 = GroupSubgroupsRecord(parent2, child2)
    val membership23 = GroupSubgroupsRecord(parent2, child3)
    val membership33 = GroupSubgroupsRecord(parent3, child3)

    // start empty

    Seq(parent1, parent2, parent3) foreach { case parent =>
      assertResult(Seq()) {
        runAndWait(loadGroupSubgroupsByParent(parent))
      }
    }

    Seq(child1, child2, child3) foreach { case child =>
      assertResult(Seq()) {
        runAndWait(loadGroupSubgroupsByChild(child))
      }
    }

    // insert memberships

    Seq(membership11, membership12, membership13, membership22, membership23, membership33) foreach { case mem =>
      assertResult(mem) {
        runAndWait(saveGroupSubgroups(mem))
      }
    }

    // query by parent

    assertResult(Seq(membership11, membership12, membership13)) {
      runAndWait(loadGroupSubgroupsByParent(parent1))
    }

    // query by child

    assertResult(Seq(membership12, membership22)) {
      runAndWait(loadGroupSubgroupsByChild(child2))
    }

    // delete by parent

    assertResult(2) {   // mem 2/2 and mem 2/3
      runAndWait(deleteGroupSubgroupsByParent(parent2))
    }

    // delete by child

    assertResult(2) {   // mem 1/3 and mem 2/3 because mem 2/3 was just deleted
      runAndWait(deleteGroupSubgroupsByChild(child3))
    }

    // delete by case class

    assertResult(2) {
      runAndWait(deleteGroupSubgroups(membership11)) + runAndWait(deleteGroupSubgroups(membership12))
    }

    // finish empty

    Seq(parent1, parent2, parent3) foreach { case parent =>
      assertResult(Seq()) {
        runAndWait(loadGroupSubgroupsByParent(parent))
      }
    }

    Seq(child1, child2, child3) foreach { case child =>
      assertResult(Seq()) {
        runAndWait(loadGroupSubgroupsByChild(child))
      }
    }
  }

}
