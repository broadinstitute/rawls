package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.scalatest.BeforeAndAfterEach

import java.util.UUID

class CompactEntityCorrectionSpec extends TestDriverComponentWithFlatSpecAndMatchers with BeforeAndAfterEach {

  import driver.api._
  // for raw sql queries
  import slickDataSource.dataAccess.compactEntityQuery.{GetUUIDResult, SetUUIDParameter}

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val ws2id = minimalTestData.workspace2.workspaceIdAsUUID
  private val q = compactEntityQuery

  // clean up corrections tables after each test
  override protected def afterEach(): Unit = {
    runAndWait(sql"delete from ATTRIBUTE_CORRECTIONS".asUpdate)
    runAndWait(sql"delete from ENTITY_CORRECTIONS".asUpdate)
  }

  behavior of "countOutstandingCorrections()"

  it should "count outstanding corrections" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $wsid, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $wsid, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        -- parent correction is Correctable; this should get counted
        (111, 'default', 'attr1', 'Reordered'),
        -- parent correction is Different; these should NOT be counted
        (222, 'default', 'attr1', 'Different'),
        (222, 'pfb', 'attr1', 'Reordered'),
        -- parent correction is Mixed; only the Reordered attr should get counted
        (333, 'default', 'attr1', 'Reordered'),
        (333, 'pfb', 'attr1', 'Different'),
        (333, 'default', 'attr2', 'TypeDifferent'),
        -- parent correction is CorrectableModified; none should get counted
        (444, 'default', 'attr1', 'Reordered'),
        (444, 'pfb', 'attr1', 'Reordered'),
        (444, 'default', 'attr2', 'Reordered'),
        (444, 'pfb', 'attr2', 'Reordered')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    val actual = runAndWait(q.countOutstandingCorrections)

    actual shouldBe 2
  }

  it should "not count any corrections if workspace has been modified since migration" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $wsid, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $wsid, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        -- parent correction is Correctable; this should get counted
        (111, 'default', 'attr1', 'Reordered'),
        -- parent correction is Different; these should NOT be counted
        (222, 'default', 'attr1', 'Different'),
        (222, 'pfb', 'attr1', 'Reordered'),
        -- parent correction is Mixed; only the Reordered attr should get counted
        (333, 'default', 'attr1', 'Reordered'),
        (333, 'pfb', 'attr1', 'Different'),
        (333, 'default', 'attr2', 'TypeDifferent'),
        -- parent correction is CorrectableModified; none should get counted
        (444, 'default', 'attr1', 'Reordered'),
        (444, 'pfb', 'attr1', 'Reordered'),
        (444, 'default', 'attr2', 'Reordered'),
        (444, 'pfb', 'attr2', 'Reordered')
           """.asUpdate)

    // insert workspace setting, using a last-updated date in the past
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', '1977-01-21')
        """.asUpdate)

    val actual = runAndWait(q.countOutstandingCorrections)

    actual shouldBe 0
  }

  behavior of "getNextCorrectionBatch()"

  it should "get the next correction batch" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $wsid, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $wsid, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        -- parent correction is Correctable; this should get counted
        (111, 'default', 'attr1', 'Reordered'),
        -- parent correction is Different; these should NOT be counted
        (222, 'default', 'attr1', 'Different'),
        (222, 'pfb', 'attr1', 'Reordered'),
        -- parent correction is Mixed; only the Reordered attr should get counted
        (333, 'default', 'attr1', 'Reordered'),
        (333, 'pfb', 'attr1', 'Different'),
        (333, 'default', 'attr2', 'TypeDifferent'),
        -- parent correction is CorrectableModified; none should get counted
        (444, 'default', 'attr1', 'Reordered'),
        (444, 'pfb', 'attr1', 'Reordered'),
        (444, 'default', 'attr2', 'Reordered'),
        (444, 'pfb', 'attr2', 'Reordered')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    val actual = runAndWait(q.getNextCorrectionBatch(20))

    actual should have size 2
    actual shouldBe List(
      EntityCorrection(111, wsid, "type1", "name1", Map()),
      EntityCorrection(333, wsid, "type3", "name3", Map())
    )
  }

  it should "respect the batch size argument" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $wsid, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $wsid, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        -- parent correction is Correctable; this should get counted
        (111, 'default', 'attr1', 'Reordered'),
        -- parent correction is Different; these should NOT be counted
        (222, 'default', 'attr1', 'Different'),
        (222, 'pfb', 'attr1', 'Reordered'),
        -- parent correction is Mixed; only the Reordered attr should get counted
        (333, 'default', 'attr1', 'Reordered'),
        (333, 'pfb', 'attr1', 'Different'),
        (333, 'default', 'attr2', 'TypeDifferent'),
        -- parent correction is CorrectableModified; none should get counted
        (444, 'default', 'attr1', 'Reordered'),
        (444, 'pfb', 'attr1', 'Reordered'),
        (444, 'default', 'attr2', 'Reordered'),
        (444, 'pfb', 'attr2', 'Reordered')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    val actual = runAndWait(q.getNextCorrectionBatch(1))

    actual should have size 1
    actual shouldBe List(
      EntityCorrection(111, wsid, "type1", "name1", Map())
    )
  }

  it should "not retrieve any corrections for batch if workspace has been modified since migration" in withMinimalTestDatabase {
    _ =>
      // insert to ENTITY_CORRECTIONS
      runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $wsid, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $wsid, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

      // insert to ATTRIBUTE_CORRECTIONS
      runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        -- parent correction is Correctable; this should get counted
        (111, 'default', 'attr1', 'Reordered'),
        -- parent correction is Different; these should NOT be counted
        (222, 'default', 'attr1', 'Different'),
        (222, 'pfb', 'attr1', 'Reordered'),
        -- parent correction is Mixed; only the Reordered attr should get counted
        (333, 'default', 'attr1', 'Reordered'),
        (333, 'pfb', 'attr1', 'Different'),
        (333, 'default', 'attr2', 'TypeDifferent'),
        -- parent correction is CorrectableModified; none should get counted
        (444, 'default', 'attr1', 'Reordered'),
        (444, 'pfb', 'attr1', 'Reordered'),
        (444, 'default', 'attr2', 'Reordered'),
        (444, 'pfb', 'attr2', 'Reordered')
           """.asUpdate)

      // insert workspace setting, using a last-updated date in the past
      runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', '1977-01-21')
        """.asUpdate)

      val actual = runAndWait(q.getNextCorrectionBatch(20))

      actual shouldBe empty
  }

  behavior of "updateWorkspaceGone()"

  it should "update entity corrections to CurrentGone" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $ws2id, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $ws2id, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    val rowsUpdated = runAndWait(q.updateWorkspaceGone(wsid))
    rowsUpdated shouldBe 2

    val actual = runAndWait(sql"""
        select id, workspace_id, status
        from ENTITY_CORRECTIONS
        """.as[(Int, UUID, String)]).toList

    actual shouldBe List(
      (111, wsid, "CurrentGone"),
      (222, ws2id, "Different"),
      (333, wsid, "CurrentGone"),
      (444, ws2id, "CorrectableModified")
    )
  }

  behavior of "updateWorkspaceModified()"

  it should "append 'Modified' to entity corrections statuses" in withMinimalTestDatabase { _ =>
    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, 'type1', 'name1', '{}', 'Correctable'),
        (222, $ws2id, 'type2', 'name2', '{}', 'Different'),
        (333, $wsid, 'type3', 'name3', '{}', 'Mixed'),
        (444, $ws2id, 'type4', 'name4', '{}', 'CorrectableModified')
           """.asUpdate)

    val rowsUpdated = runAndWait(q.updateWorkspaceModified(ws2id))
    rowsUpdated shouldBe 1

    val actual = runAndWait(sql"""
        select id, workspace_id, status
        from ENTITY_CORRECTIONS
        """.as[(Int, UUID, String)]).toList

    actual shouldBe List(
      (111, wsid, "Correctable"),
      (222, ws2id, "DifferentModified"),
      (333, wsid, "Mixed"),
      (444, ws2id, "CorrectableModified")
    )

  }

  behavior of "checkWorkspaceLastModified()"

  it should "return Some(true) if the workspace has been modified since migration" in withMinimalTestDatabase { _ =>
    // insert workspace setting, using a LAST_UPDATED in the past
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', '1977-01-21')
        """.asUpdate)

    val actual = runAndWait(q.checkWorkspaceLastModified(wsid))

    actual should contain(true)
  }

  it should "return Some(false) if the workspace has NOT been modified since migration" in withMinimalTestDatabase {
    _ =>
      // insert workspace setting
      runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

      val actual = runAndWait(q.checkWorkspaceLastModified(wsid))

      actual should contain(false)
  }

  it should "return None if the workspace does not have a migration setting" in withMinimalTestDatabase { _ =>
    val actual = runAndWait(q.checkWorkspaceLastModified(wsid))
    actual shouldBe empty
  }

  behavior of "checkWorkspaceLastWorkflowRun()"

  it should "return Some(true) if a workflow has run since migration" in withDefaultTestDatabase {
    val testWsid = testData.workspaceSuccessfulSubmission.workspaceIdAsUUID

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($testWsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    // update workflows to be more recent
    runAndWait(sql"""
                     update WORKFLOW
                     set status_last_changed = DATE_ADD(now(), INTERVAL 2 SECOND)
                     where SUBMISSION_ID in (
                      select ID
                       from SUBMISSION
                       where WORKSPACE_ID = $testWsid
                     )
        """.asUpdate)

    val actual = runAndWait(q.checkWorkspaceLastWorkflowRun(testWsid))

    actual should contain(true)
  }

  it should "return Some(false) if workflows only ran before migration" in withDefaultTestDatabase {
    val testWsid = testData.workspaceSuccessfulSubmission.workspaceIdAsUUID

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($testWsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    val actual = runAndWait(q.checkWorkspaceLastWorkflowRun(testWsid))

    actual should contain(false)
  }

  it should "return Some(false) if the workspace has no workflows" in withDefaultTestDatabase {
    val testWsid = testData.workspaceNoSubmissions.workspaceIdAsUUID

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($testWsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', now())
        """.asUpdate)

    val actual = runAndWait(q.checkWorkspaceLastWorkflowRun(testWsid))

    actual should contain(false)
  }

}
