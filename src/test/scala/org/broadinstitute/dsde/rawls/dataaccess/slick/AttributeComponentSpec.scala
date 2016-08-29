package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by dvoet on 2/9/16.
 */
class AttributeComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with AttributeComponent {
  import driver.api._

  val workspace = Workspace("broad-dsde-test", "test-workspace", None, UUID.randomUUID().toString, "fake-bucket", DateTime.now, DateTime.now, "biden", Map.empty, Map.empty, Map.empty, false)
  val workspaceId = UUID.fromString(workspace.workspaceId)

  // we don't know the IDs because it autoincrements
  val (dummyId1, dummyId2) = (12345, 67890)
  def assertExpectedRecords(expectedRecords: WorkspaceAttributeRecord*): Unit = {
    val records = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result) map { _.copy(id = dummyId1) }
    records should contain theSameElementsAs { expectedRecords map { _.copy(id = dummyId1) }}
  }

  "AttributeComponent" should "insert string attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", Option("test"), None, None, None, None, None))
  }

  it should "insert number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNumber(3.14159)
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, Option(3.14159), None, None, None, None))
  }

  it should "insert boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeBoolean(true)
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, Option(true), None, None, None))
  }

  it should "insert attribute value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(8), AttributeNumber(7), AttributeNumber(6)))
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(Seq(1, 1, 1, 1)) { numRows }

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, Option(9), None, None, Option(0), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, Option(8), None, None, Option(1), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, Option(7), None, None, Option(2), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, Option(6), None, None, Option(3), Option(4)))
  }

  it should "insert empty list" in withEmptyTestDatabase {
    val testAttribute = AttributeEmptyList
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    //NOTE: listIndex of -1 is the magic number for "empty list". see AttributeComponent.unmarshalList
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, None, Option(-1), Option(0)))
  }

  it should "save empty AttributeValueLists as AttributeEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    //NOTE: listIndex of -1 is the magic number for "empty list". see AttributeComponent.unmarshalList
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, None, Option(-1), Option(0)))
  }

  it should "save empty AttributeEntityReferenceLists as AttributeEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    //NOTE: listIndex of -1 is the magic number for "empty list". see AttributeComponent.unmarshalList
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, None, Option(-1), Option(0)))
  }

  it should "insert null attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNull
    runAndWait(workspaceQuery.save(workspace))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, None, None, None))
  }

  it should "insert entity reference attribute" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname1", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val entityId = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name", "type", workspaceId, 0, None))
    val testAttribute = AttributeEntityReference("type", "name")
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { numRows.head }

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, Option(entityId), None, None))
  }

  it should "insert entity reference attribute list" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname2", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val entityId1 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name1", "type", workspaceId, 0, None))
    val entityId2 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name2", "type", workspaceId, 0, None))
    val entityId3 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name3", "type", workspaceId, 0, None))
    val entityId4 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name4", "type", workspaceId, 0, None))

    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name1"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name4")))
    val numRows = workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(Seq(1, 1, 1, 1)) { numRows }

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, Option(entityId1), Option(0), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, Option(entityId2), Option(1), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, Option(entityId3), Option(2), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test", None, None, None, Option(entityId4), Option(3), Option(4)))

  }

  it should "throw exception inserting ref to nonexistent entity" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname3", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val testAttribute = AttributeEntityReference("type", "name")
    intercept[RawlsException] {
      workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    }
  }

  it should "throw exception inserting inconsistent list values" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeString("oops"), AttributeNumber(7), AttributeNumber(6)))
    intercept[RawlsException] {
      workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    }
  }

  it should "throw exception inserting inconsistent list references" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type1", "foo"), AttributeEntityReference("type2", "foo"), AttributeEntityReference("type1", "foo")))
    intercept[RawlsException] {
      workspaceAttributeQuery.insertAttributeRecords(workspaceId, defaultAttributeName("test"), testAttribute, workspaceId).map(x => runAndWait(x))
    }
  }

  it should "delete attribute records" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.save(workspace))
    val inserts = Seq(
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test1", None, Some(1), None, None, None, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test2", None, Some(2), None, None, None, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "test3", None, Some(3), None, None, None, None)) map { insert =>

      runAndWait(insert)
    }
    val attributeRecs = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result)
    assertResult(3) { attributeRecs.size }

    assertResult(3) { runAndWait(workspaceAttributeQuery.deleteAttributeRecords(attributeRecs)) }

    assertResult(0) { runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result).size }
  }

  it should "unmarshall attribute records" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "string", Some("value"), None, None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "num", None, Some(1), None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "bool", None, None, Some(true), None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "ref", None, None, None, Some(1), None, None)), Some(EntityRecord(0, "name", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "null", None, None, None, None, None, None)), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(1), None, None, Some(2), Some(3))), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(2), None, None, Some(1), Some(3))), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(3), None, None, Some(0), Some(3))), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "refList", None, None, None, Some(1), Some(2), Some(3))), Some(EntityRecord(0, "name1", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "refList", None, None, None, Some(2), Some(1), Some(3))), Some(EntityRecord(0, "name2", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "refList", None, None, None, Some(3), Some(0), Some(3))), Some(EntityRecord(0, "name3", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "emptyList", None, Some(1), None, None, Some(-1), Some(0))), None)
    )

    assertResult(
      Map(
        1 -> Map(
          defaultAttributeName("string") -> AttributeString("value"),
          defaultAttributeName("num") -> AttributeNumber(1),
          defaultAttributeName("bool") -> AttributeBoolean(true),
          defaultAttributeName("ref") -> AttributeEntityReference("type", "name"),
          defaultAttributeName("refList") -> AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name1"))),
          defaultAttributeName("emptyList") -> AttributeEmptyList,
          defaultAttributeName("null") -> AttributeNull),
        2 -> Map(defaultAttributeName("valList") -> AttributeValueList(Seq(AttributeNumber(3), AttributeNumber(2), AttributeNumber(1))))
      )) {
      runAndWait(workspaceAttributeQuery.unmarshalAttributes(attributeRecs))
    }
  }

  it should "throw exception unmarshalling a list without listIndex set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(1), None, None, Some(2), Some(3))), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(2), None, None, None, Some(3))), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, 1, "valList", None, Some(3), None, None, Some(0), Some(3))), None)
    )

    intercept[RawlsException] {
      val x = runAndWait(workspaceAttributeQuery.unmarshalAttributes(attributeRecs))
      println(x) // test fails without this, compiler optimization maybe?
    }
  }
}
