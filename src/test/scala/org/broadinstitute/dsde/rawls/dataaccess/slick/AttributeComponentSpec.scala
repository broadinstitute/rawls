package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by dvoet on 2/9/16.
 */
class AttributeComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with AttributeComponent {
  import driver.api._

  "AttributeComponent" should "insert string attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Seq(AttributeRecord(insertedIds.head, "test", Option("test"), None, None, None, None))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result)
    }
  }

  it should "insert number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNumber(3.14159)
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Seq(AttributeRecord(insertedIds.head, "test", None, Option(3.14159), None, None, None))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result)
    }
  }

  it should "insert boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeBoolean(true)
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Seq(AttributeRecord(insertedIds.head, "test", None, None, Option(true), None, None))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result)
    }
  }

  it should "insert attribute value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(8), AttributeNumber(7), AttributeNumber(6)))
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(4) { insertedIds.size }

    assertResult(Set(
      AttributeRecord(0, "test", None, Option(9), None, None, Option(0)),
      AttributeRecord(0, "test", None, Option(8), None, None, Option(1)),
      AttributeRecord(0, "test", None, Option(7), None, None, Option(2)),
      AttributeRecord(0, "test", None, Option(6), None, None, Option(3)))) {

      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result).map(_.copy(id=0)).toSet
    }
  }

  it should "insert empty list" in withEmptyTestDatabase {
    val testAttribute = AttributeEmptyList
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Set(AttributeRecord(0, "test", None, None, None, None, Option(-1)))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result).map(_.copy(id=0)).toSet
    }
  }

  it should "insert null attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNull
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Set(AttributeRecord(0, "test", None, None, None, None, None))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result).map(_.copy(id=0)).toSet
    }
  }

  it should "insert entity reference attribute" in withEmptyTestDatabase {
    val workspaceId = UUID.randomUUID()
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname1", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None))
    val entityId = runAndWait(entityQuery += EntityRecord(0, "name", "type", workspaceId))
    val testAttribute = AttributeEntityReference("type", "name")
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(1) { insertedIds.size }

    assertResult(Seq(AttributeRecord(insertedIds.head, "test", None, None, None, Option(entityId), None))) {
      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result)
    }
  }

  it should "insert entity reference attribute list" in withEmptyTestDatabase {
    val workspaceId = UUID.randomUUID()
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname2", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None))
    val entityId1 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name1", "type", workspaceId))
    val entityId2 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name2", "type", workspaceId))
    val entityId3 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name3", "type", workspaceId))
    val entityId4 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name4", "type", workspaceId))

    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name1"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name4")))
    val insertedIds = attributeQuery.insertAttributeRecords("test", testAttribute, workspaceId).map(x => runAndWait(x))
    assertResult(4) { insertedIds.size }

    assertResult(Set(
      AttributeRecord(0, "test", None, None, None, Option(entityId1), Option(0)),
      AttributeRecord(0, "test", None, None, None, Option(entityId2), Option(1)),
      AttributeRecord(0, "test", None, None, None, Option(entityId3), Option(2)),
      AttributeRecord(0, "test", None, None, None, Option(entityId4), Option(3)))) {

      runAndWait(attributeQuery.filter(_.id inSet insertedIds).result).map(_.copy(id=0)).toSet
    }
  }

  it should "throw exception inserting ref to nonexistent entity" in withEmptyTestDatabase {
    val workspaceId = UUID.randomUUID()
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname3", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None))
    val testAttribute = AttributeEntityReference("type", "name")
    intercept[RawlsException] {
      attributeQuery.insertAttributeRecords("test", testAttribute, workspaceId).map(x => runAndWait(x))
    }
  }

  it should "throw exception inserting inconsistent list values" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeString("oops"), AttributeNumber(7), AttributeNumber(6)))
    intercept[RawlsException] {
      attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    }
  }

  it should "throw exception inserting inconsistent list references" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type1", "foo"), AttributeEntityReference("type2", "foo"), AttributeEntityReference("type1", "foo")))
    intercept[RawlsException] {
      attributeQuery.insertAttributeRecords("test", testAttribute, UUID.randomUUID()).map(x => runAndWait(x))
    }
  }

  it should "delete attribute records" in withEmptyTestDatabase {
    val inserts = Seq(
      (attributeQuery returning attributeQuery.map(_.id)) += AttributeRecord(0, "test1", None, Some(1), None, None, None),
      (attributeQuery returning attributeQuery.map(_.id)) += AttributeRecord(0, "test2", None, Some(2), None, None, None),
      (attributeQuery returning attributeQuery.map(_.id)) += AttributeRecord(0, "test3", None, Some(3), None, None, None)) map { insert =>

      runAndWait(insert)
    }
    val attributeRecs = runAndWait(attributeQuery.filter(_.id inSet inserts).result)
    assertResult(3) { attributeRecs.size }

    assertResult(3) { runAndWait(attributeQuery.deleteAttributeRecords(attributeRecs)) }

    assertResult(0) { runAndWait(attributeQuery.filter(_.id inSet inserts).result).size }
  }

  it should "unmarshall attribute records" in withEmptyTestDatabase {
    val workspaceId = UUID.randomUUID()
    val attributeRecs = Seq(
      ((1, AttributeRecord(0, "string", Some("value"), None, None, None, None)), None),
      ((1, AttributeRecord(0, "num", None, Some(1), None, None, None)), None),
      ((1, AttributeRecord(0, "bool", None, None, Some(true), None, None)), None),
      ((1, AttributeRecord(0, "ref", None, None, None, Some(1), None)), Some(EntityRecord(0, "name", "type", workspaceId))),
      ((1, AttributeRecord(0, "null", None, None, None, None, None)), None),
      ((2, AttributeRecord(0, "valList", None, Some(1), None, None, Some(2))), None),
      ((2, AttributeRecord(0, "valList", None, Some(2), None, None, Some(1))), None),
      ((2, AttributeRecord(0, "valList", None, Some(3), None, None, Some(0))), None),
      ((1, AttributeRecord(0, "refList", None, None, None, Some(1), Some(2))), Some(EntityRecord(0, "name1", "type", workspaceId))),
      ((1, AttributeRecord(0, "refList", None, None, None, Some(2), Some(1))), Some(EntityRecord(0, "name2", "type", workspaceId))),
      ((1, AttributeRecord(0, "refList", None, None, None, Some(3), Some(0))), Some(EntityRecord(0, "name3", "type", workspaceId))),
      ((1, AttributeRecord(0, "emptyList", None, Some(1), None, None, Some(-1))), None)
    )

    assertResult(
      Map(
        1 -> Map(
          "string" -> AttributeString("value"),
          "num" -> AttributeNumber(1),
          "bool" -> AttributeBoolean(true),
          "ref" -> AttributeEntityReference("type", "name"),
          "refList" -> AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name1"))),
          "emptyList" -> AttributeEmptyList,
          "null" -> AttributeNull),
        2 -> Map("valList" -> AttributeValueList(Seq(AttributeNumber(3), AttributeNumber(2), AttributeNumber(1))))
      )) {
      attributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "throw exception unmarshalling a list without listIndex set for all" in withEmptyTestDatabase {
    val workspaceId = UUID.randomUUID()
    val attributeRecs = Seq(
      ((1 -> AttributeRecord(0, "valList", None, Some(1), None, None, Some(2))), None),
      ((1 -> AttributeRecord(0, "valList", None, Some(2), None, None, None)), None),
      ((1 -> AttributeRecord(0, "valList", None, Some(3), None, None, Some(0))), None)
    )

    intercept[RawlsException] {
      val x = attributeQuery.unmarshalAttributes(attributeRecs)
      println(x) // test fails without this, compiler optimization maybe?
    }
  }
}
