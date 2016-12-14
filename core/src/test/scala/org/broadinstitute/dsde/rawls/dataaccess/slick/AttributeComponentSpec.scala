package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by dvoet on 2/9/16.
 */
class AttributeComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with AttributeComponent with RawlsTestUtils {
  import driver.api._

  val workspace = Workspace("broad-dsde-test", "test-workspace", None, UUID.randomUUID().toString, "fake-bucket", DateTime.now, DateTime.now, "biden", Map.empty, Map.empty, Map.empty, false)
  val workspaceId = UUID.fromString(workspace.workspaceId)

  // we don't know the IDs because it autoincrements
  val (dummyId1, dummyId2) = (12345, 67890)
  def assertExpectedRecords(expectedRecords: WorkspaceAttributeRecord*): Unit = {
    val records = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result) map { _.copy(id = dummyId1) }
    assertSameElements(records, expectedRecords map { _.copy(id = dummyId1) })
  }

  def insertWorkspaceAttributeRecords(workspaceId: UUID, attributeName: AttributeName, attribute: Attribute): ReadWriteAction[Option[Int]] = {
    entityQuery.findEntityByWorkspace(workspaceId).result flatMap { entityRecords =>
      val entityIdsByRef = entityRecords.map(rec => AttributeEntityReference(rec.entityType, rec.name) -> rec.id).toMap
      workspaceAttributeQuery ++= workspaceAttributeQuery.marshalAttribute(workspaceId, attributeName, attribute, entityIdsByRef)
    }
  }

  "AttributeComponent" should "insert string attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", Option("test"), None, None, None, None, None, None))
  }


  "AttributeComponent" should "insert string attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("\"thisshouldbelegitright\"")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("\"thisshouldbelegitright\""), None, None, None))

  }

  "AttributeComponent" should "insert number attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("95")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("95"), None, None, None))

  }

  "AttributeComponent" should "insert boolean attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("true")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("true"), None, None, None))

  }

  "AttributeComponent" should "insert number list attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[9, 3]")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("[9, 3]"), None, None, None))

  }


  "AttributeComponent" should "insert mixed list attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[\"foo,\"bar\",true, 54]")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("[\"foo,\"bar\",true, 54]"), None, None, None))

  }

  "AttributeComponent" should "insert mixed list attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}"), None, None, None))

  }


  it should "insert library-namespace attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName(AttributeName.libraryNamespace, "test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.libraryNamespace, "test", Option("test"), None, None, None, None, None, None))
  }

  it should "insert number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNumber(3.14159)
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(3.14159), None, None, None, None, None))
  }

  it should "insert boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeBoolean(true)
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, Option(true), None, None, None, None))
  }

  it should "insert attribute value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(8), AttributeNumber(7), AttributeNumber(6)))
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(9), None, None, None, Option(0), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(8), None, None, None, Option(1), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(7), None, None, None, Option(2), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(6), None, None, None, Option(3), Option(4)))
  }

  it should "insert empty value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueEmptyList
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Some(-1.0), None, None, None, None, Option(0)))
  }

  it should "insert empty ref list" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceEmptyList
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, Option(0)))
  }

  it should "save empty AttributeValueLists as AttributeValueEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Some(-1.0), None, None, None, None, Option(0)))
  }

  it should "save empty AttributeEntityReferenceLists as AttributeEntityReferenceEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, Option(0)))
  }

  it should "insert null attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNull
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, None))
  }

  it should "insert entity reference attribute" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname1", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val entityId = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name", "type", workspaceId, 0, None))
    val testAttribute = AttributeEntityReference("type", "name")
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId), None, None))
  }

  it should "insert entity reference attribute list" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname2", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val entityId1 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name1", "type", workspaceId, 0, None))
    val entityId2 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name2", "type", workspaceId, 0, None))
    val entityId3 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name3", "type", workspaceId, 0, None))
    val entityId4 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name4", "type", workspaceId, 0, None))

    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name1"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name4")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId1), Option(0), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId2), Option(1), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId3), Option(2), Option(4)),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId4), Option(3), Option(4)))

  }

  it should "throw exception inserting ref to nonexistent entity" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname3", workspaceId, "bucket", defaultTimeStamp, defaultTimeStamp, "me", false, None, 0))
    val testAttribute = AttributeEntityReference("type", "name")
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "throw exception inserting inconsistent list values" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeString("oops"), AttributeNumber(7), AttributeNumber(6)))
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "throw exception inserting inconsistent list references" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type1", "foo"), AttributeEntityReference("type2", "foo"), AttributeEntityReference("type1", "foo")))
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "delete attribute records" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.save(workspace))
    val inserts = Seq(
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test1", None, Some(1), None, None, None, None, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test2", None, Some(2), None, None, None, None, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test3", None, Some(3), None, None, None, None, None)) map { insert =>

      runAndWait(insert)
    }
    val attributeRecs = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result)
    assertResult(3) { attributeRecs.size }

    assertResult(3) { runAndWait(workspaceAttributeQuery.deleteAttributeRecords(attributeRecs)) }

    assertResult(0) { runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result).size }
  }

  it should "unmarshall attribute records" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "string", Some("value"), None, None, None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.libraryNamespace, "string", Some("lib-value"), None, None, None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "num", None, Some(1), None, None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "bool", None, None, Some(true), None, None, None, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "ref", None, None, None, None, Some(1), None, None)), Some(EntityRecord(0, "name", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "null", None, None, None, None, None, None, None)), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3))), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, Some(1), Some(3))), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3))), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(1), Some(2), Some(3))), Some(EntityRecord(0, "name1", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(2), Some(1), Some(3))), Some(EntityRecord(0, "name2", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(3), Some(0), Some(3))), Some(EntityRecord(0, "name3", "type", workspaceId, 0, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "emptyList", None, Some(1), None, None, None, Some(-1), Some(0))), None)
    )

    assertResult(
      Map(
        1 -> Map(
          AttributeName.withDefaultNS("string") -> AttributeString("value"),
          AttributeName(AttributeName.libraryNamespace, "string") -> AttributeString("lib-value"),
          AttributeName.withDefaultNS("num") -> AttributeNumber(1),
          AttributeName.withDefaultNS("bool") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("ref") -> AttributeEntityReference("type", "name"),
          AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name1"))),
          AttributeName.withDefaultNS("emptyList") -> AttributeValueEmptyList,
          AttributeName.withDefaultNS("null") -> AttributeNull),
        2 -> Map(AttributeName.withDefaultNS("valList") -> AttributeValueList(Seq(AttributeNumber(3), AttributeNumber(2), AttributeNumber(1))))
      )) {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)

    }
  }

  it should "throw exception unmarshalling a list without listLength set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3))), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, Some(1), None)), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3))), None)
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "throw exception unmarshalling a list without listIndex set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3))), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, None, Some(3))), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3))), None)
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "extra data in workspace attribute temp table should not mess things up" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace = Workspace(
      "test_namespace",
      "test_name",
      None,
      workspaceId.toString,
      "bucketname",
      currentTime(),
      currentTime(),
      "me",
      Map(
        AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
        AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)),
      Map.empty,
      Map.empty,
      false)


    val updatedWorkspace = workspace.copy(attributes = Map(AttributeName.withDefaultNS("attributeString") -> AttributeString(UUID.randomUUID().toString)))

    def saveWorkspace = DbResource.dataSource.inTransaction(d => d.workspaceQuery.save(workspace))

    def updateWorkspace = {
      DbResource.dataSource.database.run(
        (this.workspaceAttributeScratchQuery += WorkspaceAttributeScratchRecord(0, workspaceId, AttributeName.defaultNamespace, "attributeString", Option("foo"), None, None, None, None, None, None, "not a transaction id")) andThen
          this.workspaceQuery.save(updatedWorkspace).transactionally andThen
          this.workspaceAttributeScratchQuery.map { r => (r.name, r.valueString) }.result.withPinnedSession
      )
    }

    assertResult(Vector(("attributeString", Some("foo")))) {
      Await.result(saveWorkspace flatMap { _ => updateWorkspace }, Duration.Inf)
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
      runAndWait(this.workspaceQuery.findById(workspaceId.toString))
    }
  }

  it should "extra data in entity attribute temp table should not mess things up" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()
    val workspace = Workspace(
      "test_namespace",
      "test_name",
      None,
      workspaceId.toString,
      "bucketname",
      currentTime(),
      currentTime(),
      "me",
      Map.empty,
      Map.empty,
      Map.empty,
      false)

    val entity = Entity("e", "et", Map(
      AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
      AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
      AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)))

    val updatedEntity = entity.copy(attributes = Map(AttributeName.withDefaultNS("attributeString") -> AttributeString(UUID.randomUUID().toString)))

    def saveWorkspace = DbResource.dataSource.inTransaction(d => d.workspaceQuery.save(workspace))
    def saveEntity = DbResource.dataSource.inTransaction(d => d.entityQuery.save(SlickWorkspaceContext(workspace), entity))

    val updateAction = for {
      entityRec <- this.entityQuery.findEntityByName(workspaceId, entity.entityType, entity.name).result
      _ <- this.entityAttributeScratchQuery += EntityAttributeScratchRecord(0, entityRec.head.id, AttributeName.defaultNamespace, "attributeString", Option("foo"), None, None, None, None, None, None, "not a transaction id")
      _ <- this.entityQuery.save(SlickWorkspaceContext(workspace), updatedEntity).transactionally
      result <- this.entityAttributeScratchQuery.map { r => (r.name, r.valueString) }.result
    } yield {
      result
    }

    assertResult(Vector(("attributeString", Some("foo")))) {
      val doIt = for {
        _ <- saveWorkspace
        _ <- saveEntity
        result <- DbResource.dataSource.database.run(updateAction.withPinnedSession)
      } yield result
      Await.result(doIt, Duration.Inf)
    }

    assertResult(Option(updatedEntity)) {
      runAndWait(this.entityQuery.get(SlickWorkspaceContext(workspace), entity.entityType, entity.name))
    }
  }

  it should "upsert" in withEmptyTestDatabase {
    // copied from WorkspaceComponent
    def insertScratchAttributes(attributeRecs: Seq[WorkspaceAttributeRecord])(transactionId: String): ReadWriteAction[Unit] = {
      workspaceAttributeScratchQuery.batchInsertAttributes(attributeRecs, transactionId)
    }

    def insertAndUpdateID(rec: WorkspaceAttributeRecord): WorkspaceAttributeRecord = {
      rec.copy(id = runAndWait((workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += rec))
    }

    runAndWait(workspaceQuery.save(workspace))

    val existing = Seq(
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId1, workspaceId, AttributeName.defaultNamespace, "test1", Option("test"), None, None, None, None, None, None)),
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test2", None, Option(2), None, None, None, None, None))
    )

    assertExpectedRecords(existing:_*)

    val update = WorkspaceAttributeRecord(2, workspaceId, AttributeName.defaultNamespace, "test2", Option("test2"), None, None, None, None, None, None)
    val insert = WorkspaceAttributeRecord(3, workspaceId, AttributeName.defaultNamespace, "test3", None, None, Option(false), None, None, None, None)
    val toSave = Seq(update, insert)

    runAndWait(workspaceAttributeQuery.upsertAction(toSave, existing, insertScratchAttributes))

    assertExpectedRecords(toSave:_*)
  }

}
