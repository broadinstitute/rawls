package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  Entity
}
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor.QuicksilverMigrationMonitorConfig
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.UUID
import scala.concurrent.duration._
import scala.language.postfixOps

class QuicksilverMigrationMonitorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually {

  def this() = this(ActorSystem("QuicksilverMigrationMonitorSpec"))

  import driver.api._
  // for raw sql queries
  import slickDataSource.dataAccess.compactEntityQuery.{GetUUIDResult, SetUUIDParameter}

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val ws2id = minimalTestData.workspace2.workspaceIdAsUUID
  private val q = compactEntityQuery

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  // define fixture entities, some of which will need to be corrected
  val currentSample1 = Entity(
    "sample-id-1",
    "sample",
    Map(
      AttributeName.withDefaultNS("reorderedNums") -> AttributeValueList(
        Seq(AttributeNumber(3), AttributeNumber(1), AttributeNumber(2))
      ),
      AttributeName.withDefaultNS("okNums") -> AttributeValueList(
        Seq(AttributeNumber(4), AttributeNumber(5), AttributeNumber(6))
      ),
      AttributeName.fromDelimitedName("pfb:reorderedStrings") -> AttributeValueList(
        Seq(AttributeString("bar"), AttributeString("foo"))
      ),
      AttributeName.withDefaultNS("scalarString") -> AttributeString("Hello world")
    )
  )
  val currentSample2 = Entity(
    "sample-id-2",
    "sample",
    Map(
      AttributeName.withDefaultNS("okNums") -> AttributeValueList(
        Seq(AttributeNumber(4), AttributeNumber(5), AttributeNumber(6))
      ),
      AttributeName.withDefaultNS("scalarString") -> AttributeString("Hello world")
    )
  )
  val currentSet = Entity(
    "set-1",
    "sample_set",
    Map(
      AttributeName.withDefaultNS("reorderedSamples") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("sample", "sample-id-2"),
          AttributeEntityReference("sample", "sample-id-1")
        )
      ),
      AttributeName.withDefaultNS("okSamples") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("sample", "sample-id-1"),
          AttributeEntityReference("sample", "sample-id-2")
        )
      )
    )
  )

  val currentEntities = Seq(currentSample1, currentSample2, currentSet)

  // define how the fixture entities should look after correction
  val correctedSample1 = Entity(
    "sample-id-1",
    "sample",
    Map(
      AttributeName.withDefaultNS("reorderedNums") -> AttributeValueList(
        Seq(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3))
      ),
      AttributeName.withDefaultNS("okNums") -> AttributeValueList(
        Seq(AttributeNumber(4), AttributeNumber(5), AttributeNumber(6))
      ),
      AttributeName.fromDelimitedName("pfb:reorderedStrings") -> AttributeValueList(
        Seq(AttributeString("foo"), AttributeString("bar"))
      ),
      AttributeName.withDefaultNS("scalarString") -> AttributeString("Hello world")
    )
  )
  val correctedSample2 = Entity(
    "sample-id-2",
    "sample",
    Map(
      AttributeName.withDefaultNS("okNums") -> AttributeValueList(
        Seq(AttributeNumber(4), AttributeNumber(5), AttributeNumber(6))
      ),
      AttributeName.withDefaultNS("scalarString") -> AttributeString("Hello world")
    )
  )
  val correctedSet = Entity(
    "set-1",
    "sample_set",
    Map(
      AttributeName.withDefaultNS("reorderedSamples") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("sample", "sample-id-1"),
          AttributeEntityReference("sample", "sample-id-2")
        )
      ),
      AttributeName.withDefaultNS("okSamples") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("sample", "sample-id-1"),
          AttributeEntityReference("sample", "sample-id-2")
        )
      )
    )
  )

  behavior of "QuicksilverMigrationMonitor"

  it should "apply all corrections" in withMinimalTestDatabase { _ =>
    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(wsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, ${correctedSample1.entityType}, ${correctedSample1.name}, ${CompactEntitySerialization
        .toSql(
          correctedSample1.attributes
        )
        .compactPrint}, 'Mixed'),
        (222, $wsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (111, 'default', 'reorderedNums', 'Reordered'),
        (111, 'default', 'okNums', 'Correct'),
        (111, 'pfb', 'reorderedStrings', 'Reordered'),
        (222, 'default', 'reorderedSamples', 'Reordered'),
        (222, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', DATE_ADD(now(), INTERVAL 2 SECOND))
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = false
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    eventually[Unit](timeout = timeout(Span(10, Seconds))) {
      val actual =
        runAndWait(q.getEntities(wsid, currentEntities.map(_.toPointer).toSet))
          .map(_.toEntity)
      actual should contain theSameElementsAs Set(correctedSample1, correctedSample2, correctedSet)
    }

    // assert ATTRIBUTE_CORRECTIONS has updated statuses. Note this uses allElementsOf, not theSameElementsAs,
    // because the monitor will insert an additional attribute for "scalarString" which is not a list.
    val actualStatuses = runAndWait(sql"""
            select correction_id, namespace, name, status
            from ATTRIBUTE_CORRECTIONS
        """.as[(Int, String, String, String)]).toSeq

    actualStatuses should contain allElementsOf Seq(
      (111, "default", "reorderedNums", "Corrected"),
      (111, "default", "okNums", "Correct"),
      (111, "pfb", "reorderedStrings", "Corrected"),
      (222, "default", "reorderedSamples", "Corrected"),
      (222, "default", "okSamples", "Correct")
    )

  }

  it should "only write to current entities that have corrections" in withMinimalTestDatabase { _ =>
    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(wsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (333, $wsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (333, 'default', 'reorderedSamples', 'Reordered'),
        (333, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', DATE_ADD(now(), INTERVAL 2 SECOND))
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = false
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    eventually[Unit](timeout = timeout(Span(10, Seconds))) {
      val actual =
        runAndWait(q.getEntities(wsid, currentEntities.map(_.toPointer).toSet))
          .map(_.toEntity)
      actual should contain theSameElementsAs Set(currentSample1, currentSample2, correctedSet)
    }

    // assert ATTRIBUTE_CORRECTIONS has updated statuses.
    val actualStatuses = runAndWait(sql"""
            select correction_id, namespace, name, status
            from ATTRIBUTE_CORRECTIONS
        """.as[(Int, String, String, String)]).toSeq

    actualStatuses should contain theSameElementsAs Seq(
      (333, "default", "reorderedSamples", "Corrected"),
      (333, "default", "okSamples", "Correct")
    )

  }

  it should "apply no corrections for a dry run" in withMinimalTestDatabase { _ =>
    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(wsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (444, $wsid, ${correctedSample1.entityType}, ${correctedSample1.name}, ${CompactEntitySerialization
        .toSql(
          correctedSample1.attributes
        )
        .compactPrint}, 'Mixed'),
        (555, $wsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (444, 'default', 'reorderedNums', 'Reordered'),
        (444, 'default', 'okNums', 'Correct'),
        (444, 'pfb', 'reorderedStrings', 'Reordered'),
        (555, 'default', 'reorderedSamples', 'Reordered'),
        (555, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', DATE_ADD(now(), INTERVAL 2 SECOND))
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = true
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    // give it 4 seconds to ensure nothing happens
    Thread.sleep(4 * 1000)

    val actual =
      runAndWait(q.getEntities(wsid, currentEntities.map(_.toPointer).toSet))
        .map(_.toEntity)
    actual should contain theSameElementsAs currentEntities

    // assert ATTRIBUTE_CORRECTIONS has not changed any statuses. Note this uses allElementsOf, not theSameElementsAs,
    // because the monitor will insert an additional attribute for "scalarString" which is not a list.
    val actualStatuses = runAndWait(sql"""
            select correction_id, namespace, name, status
            from ATTRIBUTE_CORRECTIONS
        """.as[(Int, String, String, String)]).toSeq

    actualStatuses should contain allElementsOf Seq(
      (444, "default", "reorderedNums", "Reordered"),
      (444, "default", "okNums", "Correct"),
      (444, "pfb", "reorderedStrings", "Reordered"),
      (555, "default", "reorderedSamples", "Reordered"),
      (555, "default", "okSamples", "Correct")
    )

  }

  it should "apply no corrections if the workspace has been modified since migration" in withMinimalTestDatabase { _ =>
    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(wsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (777, $wsid, ${correctedSample1.entityType}, ${correctedSample1.name}, ${CompactEntitySerialization
        .toSql(
          correctedSample1.attributes
        )
        .compactPrint}, 'Mixed'),
        (888, $wsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (777, 'default', 'reorderedNums', 'Reordered'),
        (777, 'default', 'okNums', 'Correct'),
        (777, 'pfb', 'reorderedStrings', 'Reordered'),
        (888, 'default', 'reorderedSamples', 'Reordered'),
        (888, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting, using a date in the past
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', '1977-01-21')
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = true
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    // give it 4 seconds to ensure nothing happens
    Thread.sleep(4 * 1000)

    val actual =
      runAndWait(q.getEntities(wsid, currentEntities.map(_.toPointer).toSet))
        .map(_.toEntity)
    actual should contain theSameElementsAs currentEntities
  }

  it should "apply no corrections if the workspace has run a workflow since migration" in withDefaultTestDatabase {
    val testWsid = testData.workspaceSuccessfulSubmission.workspaceIdAsUUID

    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(testWsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (999, $testWsid, ${correctedSample1.entityType}, ${correctedSample1.name}, ${CompactEntitySerialization
        .toSql(
          correctedSample1.attributes
        )
        .compactPrint}, 'Mixed'),
        (100, $testWsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (999, 'default', 'reorderedNums', 'Reordered'),
        (999, 'default', 'okNums', 'Correct'),
        (999, 'pfb', 'reorderedStrings', 'Reordered'),
        (100, 'default', 'reorderedSamples', 'Reordered'),
        (100, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($testWsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', DATE_ADD(now(), INTERVAL 2 SECOND))
        """.asUpdate)

    // update workflows to be more recent
    runAndWait(sql"""
                     update WORKFLOW
                     set status_last_changed = DATE_ADD(now(), INTERVAL 5 SECOND)
                     where SUBMISSION_ID in (
                      select ID
                       from SUBMISSION
                       where WORKSPACE_ID = $testWsid
                     )
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = true
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    // give it 4 seconds to ensure nothing happens
    Thread.sleep(4 * 1000)

    val actual =
      runAndWait(q.getEntities(testWsid, currentEntities.map(_.toPointer).toSet))
        .map(_.toEntity)
    actual should contain theSameElementsAs currentEntities

    // assert ENTITY_CORRECTIONS has "*Modified" statuses.
    val actualStatuses = runAndWait(sql"""
            select id, status
            from ENTITY_CORRECTIONS
        """.as[(Int, String)]).toSeq

    actualStatuses should contain theSameElementsAs Seq(
      (999, "MixedModified"),
      (100, "MixedModified")
    )

  }

  it should "save the uncorrected entity attributes before correcting" in withMinimalTestDatabase { _ =>
    // create entities, some of which need to be corrected
    runAndWait(q.batchWriteEntities(wsid, currentEntities, insertOnly = true))

    // insert to ENTITY_CORRECTIONS
    runAndWait(sql"""
       insert into ENTITY_CORRECTIONS(id, workspace_id, entity_type, name, attributes, status)
       values
        (111, $wsid, ${correctedSample1.entityType}, ${correctedSample1.name}, ${CompactEntitySerialization
        .toSql(
          correctedSample1.attributes
        )
        .compactPrint}, 'Mixed'),
        (222, $wsid, ${correctedSet.entityType}, ${correctedSet.name}, ${CompactEntitySerialization
        .toSql(
          correctedSet.attributes
        )
        .compactPrint}, 'Mixed')
           """.asUpdate)

    // insert to ATTRIBUTE_CORRECTIONS
    runAndWait(sql"""
       insert into ATTRIBUTE_CORRECTIONS(correction_id, namespace, name, status)
       values
        (111, 'default', 'reorderedNums', 'Reordered'),
        (111, 'default', 'okNums', 'Correct'),
        (111, 'pfb', 'reorderedStrings', 'Reordered'),
        (222, 'default', 'reorderedSamples', 'Reordered'),
        (222, 'default', 'okSamples', 'Correct')
           """.asUpdate)

    // insert workspace setting
    runAndWait(sql"""
            insert into WORKSPACE_SETTINGS(WORKSPACE_ID, SETTING_TYPE, STATUS, CONFIG, USER_ID, LAST_UPDATED)
            values ($wsid, 'CompactDataTables', 'Applied', '{}', 'fake-user', DATE_ADD(now(), INTERVAL 2 SECOND))
        """.asUpdate)

    val monitorConfig = QuicksilverMigrationMonitorConfig(
      startupDelay = 100 milliseconds,
      completionInterval = 2 hours,
      pollInterval = 100 milliseconds,
      batchTimeout = 2 seconds,
      batchSize = 20,
      dryRun = false
    )
    system.actorOf(QuicksilverMigrationMonitor.props(monitorConfig, slickDataSource))

    eventually[Unit](timeout = timeout(Span(10, Seconds))) {
      val actual =
        runAndWait(q.getEntities(wsid, currentEntities.map(_.toPointer).toSet))
          .map(_.toEntity)
      actual should contain theSameElementsAs Set(correctedSample1, correctedSample2, correctedSet)
    }

    // assert the original entities were backed up
    val backedUpEntities = runAndWait(sql"""
        select entity_type, name, corrected_at, history
        from ENTITY_CORRECTIONS
        """.as[(String, String, java.sql.Timestamp, Option[String])])

    backedUpEntities foreach { case (entityType, name, correctedAt, history) =>
      // history should be populated
      history should not be empty
      // correction date should be within the last 10 seconds (generous!)
      val datediff = (java.time.Instant.now().toEpochMilli - correctedAt.toInstant.toEpochMilli).toInt
      datediff should be > 0
      datediff should be < 10 * 1000
      // reconstitute the historical entity
      val historicalEntity = Entity(name, entityType, CompactEntitySerialization.fromSql(history))
      // find it within our original entities
      val foundEntities =
        currentEntities.filter(e => e.name == historicalEntity.name && e.entityType == historicalEntity.entityType)
      foundEntities should have size 1
      // compare
      historicalEntity shouldBe foundEntities.head

    }

  }

}
