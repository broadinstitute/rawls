package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.WorkspaceFeatureFlag

class WorkspaceFeatureFlagComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // exemplar flags for tests below
  val flagsForWorkspace1 = List(
    WorkspaceFeatureFlag("one"),
    WorkspaceFeatureFlag("two"),
    WorkspaceFeatureFlag("three")
  )

  val flagsForWorkspace2 = List(
    WorkspaceFeatureFlag("one"),
    WorkspaceFeatureFlag("two"),
    WorkspaceFeatureFlag("three"),
    WorkspaceFeatureFlag("four")
  )

  behavior of "WorkspaceFeatureFlagComponent"

  it should "save individual flags via save()" in withMinimalTestDatabase { _ =>
    flagsForWorkspace1.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.save(minimalTestData.workspace.workspaceIdAsUUID, flag))
    }
    flagsForWorkspace2.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.save(minimalTestData.workspace2.workspaceIdAsUUID, flag))
    }

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2
  }

  it should "error when inserting pre-existing flags via save()" in withMinimalTestDatabase { _ =>
    // save the exemplar flags
    flagsForWorkspace1.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.save(minimalTestData.workspace.workspaceIdAsUUID, flag))
    }
    // ensure they saved
    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    // attempt to re-save one of the flags, should error
    val ex = intercept[Exception] {
      runAndWait(
        workspaceFeatureFlagQuery.save(minimalTestData.workspace.workspaceIdAsUUID, WorkspaceFeatureFlag("two"))
      )
    }
    ex.getMessage should startWith("Duplicate entry")
  }

  it should "batch-save multiple flags via saveAll()" in withMinimalTestDatabase { _ =>
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2))

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2
  }

  it should "error when inserting pre-existing flags via saveAll()" in withMinimalTestDatabase { _ =>
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))
    // attempt to re-save one of the flags, should error
    val ex = intercept[Exception] {
      runAndWait(
        workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1.tail)
      )
    }
    ex.getMessage should startWith("Duplicate entry")
  }

  it should "return an empty list when listing all flags for a workspace if none exist" in withMinimalTestDatabase {
    _ =>
      // save some flags into workspace *ONE*
      runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))

      // list flags for workspace *TWO*, should not find any for workspace one
      val actualFlags2 =
        runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

      actualFlags2 shouldBe empty
  }

  it should "return all flags when listing all flags for a workspace" in withMinimalTestDatabase { _ =>
    // N.B. the save()* unit tests also verify the same functionality

    // save some flags into workspace one
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))

    // list flags for workspace one, should not find any for workspace one
    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
  }

  it should "return an empty list when listing specific flags for a workspace if none exist" in withMinimalTestDatabase {
    _ =>
      // save some flags into workspace *ONE*
      runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))

      // list flags for workspace *TWO*, should not find any for workspace one
      val actualFlags2 = runAndWait(
        workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID,
                                                        List("two", "three").map(WorkspaceFeatureFlag)
        )
      )

      actualFlags2 shouldBe empty
  }

  it should "return an empty list when listing specific flags for a workspace if those flags do not exist" in withMinimalTestDatabase {
    _ =>
      // save some flags into workspace *ONE*
      runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))

      // list flags for workspace one, but the wrong flags
      val actualFlags1 = runAndWait(
        workspaceFeatureFlagQuery.listFlagsForWorkspace(
          minimalTestData.workspace.workspaceIdAsUUID,
          List("these", "flags", "don't", "exist").map(WorkspaceFeatureFlag)
        )
      )

      actualFlags1 shouldBe empty
  }

  it should "return the requested flags when listing specific flags for a workspace" in withMinimalTestDatabase { _ =>
    // save some flags into workspace two
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2))

    val flagNamesToFind = List("two", "four").map(WorkspaceFeatureFlag)

    // list flags for workspace one, but only specific ones
    val actualFlags2 = runAndWait(
      workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID, flagNamesToFind)
    )

    val expectedFlags = flagsForWorkspace2.filter(flag => flagNamesToFind.contains(flag))
    expectedFlags should contain theSameElementsAs flagNamesToFind

    actualFlags2 should contain theSameElementsAs expectedFlags
  }

  it should "insert non-existent flags via saveOrUpdate" in withMinimalTestDatabase { _ =>
    flagsForWorkspace1.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace.workspaceIdAsUUID, flag))
    }
    flagsForWorkspace2.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace2.workspaceIdAsUUID, flag))
    }

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2
  }

  it should "update pre-existing flags via saveOrUpdate" in withMinimalTestDatabase { _ =>
    flagsForWorkspace1.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace.workspaceIdAsUUID, flag))
    }
    flagsForWorkspace2.foreach { flag =>
      runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace2.workspaceIdAsUUID, flag))
    }

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2

    // now, update flag "one" for workspace 1 and flag "two" for workspace 2
    val update1 = WorkspaceFeatureFlag("one")
    val update2 = WorkspaceFeatureFlag("two")

    runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace.workspaceIdAsUUID, update1))
    runAndWait(workspaceFeatureFlagQuery.saveOrUpdate(minimalTestData.workspace2.workspaceIdAsUUID, update2))

    val actualUpdated1 = runAndWait(
      workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace.workspaceIdAsUUID,
                                                      List("one", "three").map(WorkspaceFeatureFlag)
      )
    )
    val actualUpdated2 = runAndWait(
      workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID,
                                                      List("two", "four").map(WorkspaceFeatureFlag)
      )
    )

    actualUpdated1 should have size 2
    actualUpdated2 should have size 2

    // validate that we updated the flags we meant to update, but did not update another flag
    actualUpdated1 should contain theSameElementsAs List(update1, flagsForWorkspace1.last)
    actualUpdated2 should contain theSameElementsAs List(update2, flagsForWorkspace2.last)
  }

  it should "insert non-existent flags via saveOrUpdateAll" in withMinimalTestDatabase { _ =>
    runAndWait(
      workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1)
    )
    runAndWait(
      workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2)
    )

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2
  }

  it should "update pre-existing flags via saveOrUpdateAll" in withMinimalTestDatabase { _ =>
    runAndWait(
      workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1)
    )
    runAndWait(
      workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2)
    )

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should contain theSameElementsAs flagsForWorkspace1
    actualFlags2 should contain theSameElementsAs flagsForWorkspace2

    // now, update flags "one" and "two" for workspace 1 and flags "two" and "three" for workspace 2
    val update1 = List(
      WorkspaceFeatureFlag("one"),
      WorkspaceFeatureFlag("two")
    )
    val update2 = List(
      WorkspaceFeatureFlag("two"),
      WorkspaceFeatureFlag("three")
    )

    runAndWait(workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace.workspaceIdAsUUID, update1))
    runAndWait(workspaceFeatureFlagQuery.saveOrUpdateAll(minimalTestData.workspace2.workspaceIdAsUUID, update2))

    val actualUpdated1 = runAndWait(
      workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace.workspaceIdAsUUID,
                                                      List("one", "two", "three").map(WorkspaceFeatureFlag)
      )
    )
    val actualUpdated2 = runAndWait(
      workspaceFeatureFlagQuery.listFlagsForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID,
                                                      List("two", "three", "four").map(WorkspaceFeatureFlag)
      )
    )

    // validate that we updated the flags we meant to update, but did not update another flag
    actualUpdated1 should contain theSameElementsAs (update1 :+ flagsForWorkspace1.last)
    actualUpdated2 should contain theSameElementsAs (update2 :+ flagsForWorkspace2.last)
  }

  it should "delete all flags for a workspace" in withMinimalTestDatabase { _ =>
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2))

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should have size 3
    actualFlags2 should have size 4

    val deleteCount =
      runAndWait(workspaceFeatureFlagQuery.deleteAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))

    deleteCount shouldBe 3

    val actualFlagsPostDelete1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlagsPostDelete2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlagsPostDelete1 shouldBe empty
    actualFlagsPostDelete2 should have size 4
  }

  it should "noop when deleting all flags for a workspace if none exist" in withMinimalTestDatabase { _ =>
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2))

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 shouldBe empty
    actualFlags2 should have size 4

    val deleteCount =
      runAndWait(workspaceFeatureFlagQuery.deleteAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))

    deleteCount shouldBe 0

    val actualFlagsPostDelete1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlagsPostDelete2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlagsPostDelete1 shouldBe empty
    actualFlagsPostDelete2 should have size 4
  }

  it should "delete specific flags for a workspace" in withMinimalTestDatabase { _ =>
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace.workspaceIdAsUUID, flagsForWorkspace1))
    runAndWait(workspaceFeatureFlagQuery.saveAll(minimalTestData.workspace2.workspaceIdAsUUID, flagsForWorkspace2))

    val actualFlags1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlags2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlags1 should have size 3
    actualFlags2 should have size 4

    val deleteCount = runAndWait(
      workspaceFeatureFlagQuery.deleteFlagsForWorkspace(minimalTestData.workspace.workspaceIdAsUUID,
                                                        List("two", "this flag doesn't exist")
      )
    )

    deleteCount shouldBe 1

    val actualFlagsPostDelete1 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace.workspaceIdAsUUID))
    val actualFlagsPostDelete2 =
      runAndWait(workspaceFeatureFlagQuery.listAllForWorkspace(minimalTestData.workspace2.workspaceIdAsUUID))

    actualFlagsPostDelete1 should have size 2
    actualFlagsPostDelete2 should have size 4
  }

}
