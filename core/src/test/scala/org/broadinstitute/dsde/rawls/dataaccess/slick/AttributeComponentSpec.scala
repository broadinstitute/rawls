package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.rawls.dataaccess.AttributeTempTableType

import java.util.UUID
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import org.joda.time.DateTime
import spray.json.{JsObject, JsString}

import java.util.concurrent.Executors
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Created by dvoet on 2/9/16.
 */
//noinspection EmptyParenMethodAccessedAsParameterless,RedundantDefaultArgument,NameBooleanParameters,MutatorLikeMethodIsParameterless,ScalaUnnecessaryParentheses,ScalaUnusedSymbol,TypeAnnotation
class AttributeComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with AttributeComponent with RawlsTestUtils {
  import driver.api._

  val workspace = Workspace("broad-dsde-test", "test-workspace", UUID.randomUUID().toString, "fake-bucket", Some("workflow-collection"), DateTime.now, DateTime.now, "biden", Map.empty, false)
  val workspaceId = UUID.fromString(workspace.workspaceId)

  // we don't know the IDs because it autoincrements
  val (dummyId1, dummyId2) = (12345, 67890)
  def assertExpectedRecords(expectedRecords: WorkspaceAttributeRecord*): Unit = {
    val records = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result) map { _.copy(id = dummyId1) }
    assertSameElements(records, expectedRecords map { _.copy(id = dummyId1) })
  }

  def insertWorkspaceAttributeRecords(workspaceId: UUID, attributeName: AttributeName, attribute: Attribute): ReadWriteAction[Option[Int]] = {
    entityQuery.findActiveEntityByWorkspace(workspaceId).result flatMap { entityRecords =>
      val entityRecordsMap = entityRecords.map(e => e.toReference -> e.id).toMap
      workspaceAttributeQuery ++= workspaceAttributeQuery.marshalAttribute(workspaceId, attributeName, attribute, entityRecordsMap)
    }
  }

  "AttributeComponent" should "insert string attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", Option("test"), None, None, None, None, None, None, false, None))
  }

  "AttributeComponent" should "insert string attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("\"thisshouldbelegitright\"")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("\"thisshouldbelegitright\""), None, None, None, false, None))
  }

  it should "insert library-namespace attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName(AttributeName.libraryNamespace, "test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.libraryNamespace, "test", Option("test"), None, None, None, None, None, None, false, None))
  }

  it should "insert number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNumber(3.14159)
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(3.14159), None, None, None, None, None, false, None))
  }

  it should "insert json number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("95")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("95"), None, None, None, false, None))
  }

  it should "insert boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeBoolean(true)
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, Option(true), None, None, None, None, false, None))
  }

  it should "insert json boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("true")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("true"), None, None, None, false, None))
  }

  it should "insert attribute value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(8), AttributeNumber(7), AttributeNumber(6)))
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(9), None, None, None, Option(0), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(8), None, None, None, Option(1), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(7), None, None, None, Option(2), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Option(6), None, None, None, Option(3), Option(4), false, None))
  }


  "AttributeComponent" should "insert json number list attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[9,3]")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("[9,3]"), None, None, None, false, None))
  }

  it should "insert json mixed list attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[\"foo\",\"bar\",true,54]")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("[\"foo\",\"bar\",true,54]"), None, None, None, false, None))
  }

  it should "insert empty value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueEmptyList
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Some(-1.0), None, None, None, None, Option(0), false, None))
  }

  it should "insert empty ref list" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceEmptyList
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, Option(0), false, None))
  }

  it should "save empty AttributeValueLists as AttributeValueEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, Some(-1.0), None, None, None, None, Option(0), false, None))
  }

  it should "save empty AttributeEntityReferenceLists as AttributeEntityReferenceEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq())
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, Option(0), false, None))
  }

  it should "insert null attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNull
    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, None, None, None, false, None))
  }

  it should "insert entity reference attribute" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname1", workspaceId, "bucket", Some("workflow-collection"), defaultTimeStamp, defaultTimeStamp, defaultTimeStamp, "me", false, 0, WorkspaceVersions.V1.value, "gp"))
    val entityId = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name", "type", workspaceId, 0, deleted = false, None))
    val testAttribute = AttributeEntityReference("type", "name")
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId), None, None, false, None))
  }

  it should "insert entity reference attribute list" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname2", workspaceId, "bucket", Some("workflow-collection"), defaultTimeStamp, defaultTimeStamp, defaultTimeStamp, "me", false, 0, WorkspaceVersions.V1.value, "gp"))
    val entityId1 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name1", "type", workspaceId, 0, deleted = false, None))
    val entityId2 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name2", "type", workspaceId, 0, deleted = false, None))
    val entityId3 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name3", "type", workspaceId, 0, deleted = false, None))
    val entityId4 = runAndWait((entityQuery returning entityQuery.map(_.id)) += EntityRecord(0, "name4", "type", workspaceId, 0, deleted = false, None))

    val testAttribute = AttributeEntityReferenceList(Seq(AttributeEntityReference("type", "name1"), AttributeEntityReference("type", "name2"), AttributeEntityReference("type", "name3"), AttributeEntityReference("type", "name4")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId1), Option(0), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId2), Option(1), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId3), Option(2), Option(4), false, None),
      WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, None, Option(entityId4), Option(3), Option(4), false, None))

  }

  it should "throw exception inserting ref to nonexistent entity" in withEmptyTestDatabase {
    runAndWait(workspaceQuery += WorkspaceRecord("testns", "testname3", workspaceId, "bucket", Some("workflow-collection"), defaultTimeStamp, defaultTimeStamp, defaultTimeStamp, "me", false, 0, WorkspaceVersions.V1.value, "gp"))
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

  it should "insert json object attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}")

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test", None, None, None, Option("{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}"), None, None, None, false, None))

  }

  it should "delete attribute records" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.save(workspace))
    val inserts = Seq(
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test1", None, Some(1), None, None, None, None, None, false, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test2", None, Some(2), None, None, None, None, None, false, None),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test3", None, Some(3), None, None, None, None, None, false, None)) map { insert =>

      runAndWait(insert)
    }
    val attributeRecs = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result)
    assertResult(3) { attributeRecs.size }

    assertResult(3) { runAndWait(workspaceAttributeQuery.deleteAttributeRecordsById(attributeRecs.map(_.id))) }

    assertResult(0) { runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result).size }
  }

  it should "unmarshal attribute records" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "string", Some("value"), None, None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.libraryNamespace, "string", Some("lib-value"), None, None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "num", None, Some(1), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "bool", None, None, Some(true), None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "ref", None, None, None, None, Some(1), None, None, false, None)), Some(EntityRecord(0, "name", "type", workspaceId, 0, deleted = false, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "null", None, None, None, None, None, None, None, false, None)), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3), false, None)), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, Some(1), Some(3), false, None)), None),
      ((2, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3), false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(1), Some(2), Some(3), false, None)), Some(EntityRecord(0, "name1", "type", workspaceId, 0, deleted = false, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(2), Some(1), Some(3), false, None)), Some(EntityRecord(0, "name2", "type", workspaceId, 0, deleted = false, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "refList", None, None, None, None, Some(3), Some(0), Some(3), false, None)), Some(EntityRecord(0, "name3", "type", workspaceId, 0, deleted = false, None))),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "emptyList", None, Some(1), None, None, None, Some(-1), Some(0), false, None)), None)
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

  it should "unmarshal ints and doubles with appropriate precision" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "int", None, Some(2), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "negInt", None, Some(-2), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "doubleRounded", None, Some(2.0), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "negDoubleRounded", None, Some(-2.0), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "double", None, Some(2.34), None, None, None, None, None, false, None)), None),
      ((1, WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "negDouble", None, Some(-2.34), None, None, None, None, None, false, None)), None)
    )

    val unmarshalled = workspaceAttributeQuery.unmarshalAttributes(attributeRecs)

    assertResult(
      Map(
        1 -> Map(
          AttributeName.withDefaultNS("int") -> AttributeNumber(2),
          AttributeName.withDefaultNS("negInt") -> AttributeNumber(-2),
          AttributeName.withDefaultNS("doubleRounded") -> AttributeNumber(2),
          AttributeName.withDefaultNS("negDoubleRounded") -> AttributeNumber(-2),
          AttributeName.withDefaultNS("double") -> AttributeNumber(2.34),
          AttributeName.withDefaultNS("negDouble") -> AttributeNumber(-2.34)
      ))) { unmarshalled }

    val attrs = unmarshalled(1)

    def assertScale(key: String, expectedScale: Int) = {
      attrs(AttributeName.withDefaultNS(key)) match {
        case num:AttributeNumber => assertResult(expectedScale, s"scale of $key attribute") {num.value.scale}
        case _ => fail(s"expected AttributeNumber for $key")
      }
    }

    assertScale("int", 0)
    assertScale("negInt", 0)
    assertScale("doubleRounded", 0)
    assertScale("negDoubleRounded", 0)
    assertScale("double", 2)
    assertScale("negDouble", 2)
  }

  it should "throw exception unmarshalling a list without listLength set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3), false, None)), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, Some(1), None, false, None)), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3), false, None)), None)
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "throw exception unmarshalling a list without listIndex set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(1), None, None, None, Some(2), Some(3), false, None)), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(2), None, None, None, None, Some(3), false, None)), None),
      ((1 -> WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "valList", None, Some(3), None, None, None, Some(0), Some(3), false, None)), None)
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  List(8, 17, 32, 65, 128, 257) foreach { parallelism =>
    it should s"handle $parallelism simultaneous writes to their own ws attribute temp tables" in withEmptyTestDatabase {
      var threadIds: TrieMap[String, Long] = TrieMap.empty

      // we test saving entities across two workspaces, which we'll call "odd" and "even"
      val workspaceIdOdd: UUID = UUID.randomUUID()
      val workspaceIdEven: UUID = UUID.randomUUID()

      val workspaceOdd = Workspace(
        "test_namespace", "workspaceOdd", workspaceIdOdd.toString, "bucketname", Some("workflow-collection"),
        currentTime(), currentTime(), "me", Map.empty, false)
      val workspaceEven = Workspace(
        "test_namespace", "workspaceEven", workspaceIdEven.toString, "bucketname", Some("workflow-collection"),
        currentTime(), currentTime(), "me", Map.empty, false)

      def isOdd(i: Int) = i % 2 == 1

      def isEven(i: Int) = !isOdd(i)

      def saveWsAttributes(attrs: Map[AttributeName, Attribute], idx: Int): Future[Workspace] = this.synchronized {
        DbResource.dataSource.inTransactionWithAttrTempTable({ d =>
          val targetWorkspace: Workspace = if (isOdd(idx))
            runAndWait(d.workspaceQuery.findById(workspaceOdd.workspaceId)).get
          else
            runAndWait(d.workspaceQuery.findById(workspaceEven.workspaceId)).get

          d.workspaceQuery.save(targetWorkspace.copy(attributes = attrs))
        }, Set(AttributeTempTableType.Workspace))
      }


      // create the entities-to-be-saved
      val entitiesToSave: Map[Int, Map[AttributeName, Attribute]] = (1 to parallelism).map { idx =>
        (idx, Map(AttributeName.withDefaultNS("uuid_attr") -> AttributeString(UUID.randomUUID().toString),
          AttributeName.withDefaultNS("random_string") -> AttributeString(RandomStringUtils.random(parallelism))))
      }.toMap
      withClue(s"should have prepped $parallelism entities to save") {
        entitiesToSave.size shouldBe (parallelism)
      }

      // save the workspaces
      val savedWorkspaceOdd = runAndWait(workspaceQuery.save(workspaceOdd))
      withClue("workspace (odd) should have saved") {
        savedWorkspaceOdd.workspaceId shouldBe (workspaceIdOdd.toString)
      }
      val savedWorkspaceEven = runAndWait(workspaceQuery.save(workspaceEven))
      withClue("workspace (even) should have saved") {
        savedWorkspaceEven.workspaceId shouldBe (workspaceIdEven.toString)
      }

      // set up multithreaded context, but saveWsAttributes needs to only allow one thread at a time
      implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

      val saveQueries = entitiesToSave map { case (k, v) => saveWsAttributes(v, k) }
      val parallelSaves = scala.concurrent.Future.traverse(saveQueries) { f =>
        f.map {
          f.map { ws =>
            threadIds += (s"${ws.name}" -> Thread.currentThread().getId)
            ws
          }
          Success(_)
        }.recover { case e => Failure(e) }(executionContext)
      }
      val saveAttempts = Await.result(parallelSaves, Duration.Inf)

      val saveFailures = saveAttempts.collect { case Failure(ex) => ex }
      val savedEntities = saveAttempts.collect { case Success(e) => e }

      withClue("should not have any failures during save") {
        saveFailures shouldBe empty
      }
      withClue(s"should have completed exactly $parallelism workspace saves") {
        saveFailures shouldBe empty
      }

      val expectedCountOdd = Math.ceil(parallelism.toDouble / 2d)
      val expectedCountEven = Math.floor(parallelism.toDouble / 2d)

      val evenVersionCount = Await.result(DbResource.dataSource.database.run(workspaceQuery.findByIdQuery(workspaceIdEven).result.map {
        _.head
      }), Duration.Inf).recordVersion
      val oddVersionCount = Await.result(DbResource.dataSource.database.run(workspaceQuery.findByIdQuery(workspaceIdOdd).result.map {
        _.head
      }), Duration.Inf).recordVersion


      withClue(s"We should save the odd workspace $expectedCountOdd times") {
        oddVersionCount shouldBe expectedCountOdd
      }
      withClue(s"We should save the even workspace $expectedCountEven times") {
        evenVersionCount shouldBe expectedCountEven
      }

      //multiple unique thread ids--this assertion is failing periodically, what does that mean?
//      assert(threadIds.values.toSet.size > 1)
    }
  }


  List(8, 17, 32, 65, 128, 257) foreach { parallelism =>
    it should s"handle $parallelism simultaneous writes to their own entity attribute temp tables" in withEmptyTestDatabase {

      var threadIds: TrieMap[String, Long] = TrieMap.empty

      // we test saving entities across two workspaces, which we'll call "odd" and "even"
      val workspaceIdOdd: UUID = UUID.randomUUID()
      val workspaceIdEven: UUID = UUID.randomUUID()

      val workspaceOdd = Workspace(
        "test_namespace", "workspaceOdd", workspaceIdOdd.toString, "bucketname", Some("workflow-collection"),
        currentTime(), currentTime(), "me", Map.empty, false)
      val workspaceEven = Workspace(
        "test_namespace", "workspaceEven", workspaceIdEven.toString, "bucketname", Some("workflow-collection"),
        currentTime(), currentTime(), "me", Map.empty, false)

      def isOdd(i: Int) = i % 2 == 1
      def isEven(i: Int) = !isOdd(i)

      val entityType: String = "et"

      def saveEntity(e: Entity): Future[Entity] = DbResource.dataSource.inTransactionWithAttrTempTable ({ d =>
        val targetWorkspace = if (isOdd(e.name.toInt))
          workspaceOdd
        else
          workspaceEven
        d.entityQuery.save(targetWorkspace, e)
      }, Set(AttributeTempTableType.Entity))

      // create the entities-to-be-saved
      val entitiesToSave:Map[Int, Entity] = (1 to parallelism).map { idx =>
        (idx, Entity(s"$idx", entityType, Map(
          AttributeName.withDefaultNS("uuid") -> AttributeString(UUID.randomUUID().toString),
          AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159))))
      }.toMap
      withClue(s"should have prepped $parallelism entities to save"){entitiesToSave.size shouldBe (parallelism)}

      // save the workspaces
      val savedWorkspaceOdd = runAndWait(workspaceQuery.save(workspaceOdd))
      withClue("workspace (odd) should have saved") {savedWorkspaceOdd.workspaceId shouldBe (workspaceIdOdd.toString)}
      val savedWorkspaceEven = runAndWait(workspaceQuery.save(workspaceEven))
      withClue("workspace (even) should have saved") {savedWorkspaceEven.workspaceId shouldBe (workspaceIdEven.toString)}

      // in parallel, save entities to the workspace
      // set up an execution context with $parallelism threads
      // some futures will execute on the same threads but we ensure
      // at least some parallelism and validate that in a later assert statement
      implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

      val saveQueries = entitiesToSave.values map saveEntity
      val parallelSaves = scala.concurrent.Future.traverse(saveQueries) { f =>
        f.map {
          f.map { entity =>
            threadIds += (s"${entity.entityType}/${entity.name}" -> Thread.currentThread().getId)
            entity
          }
          Success(_)
        }.recover { case e => Failure(e) }(executionContext)
      }
      val saveAttempts =  Await.result(parallelSaves, Duration.Inf)

      val saveFailures = saveAttempts.collect { case Failure(ex) => ex }
      val savedEntities = saveAttempts.collect { case Success(e) => e }

      withClue("should not have any failures during save") { saveFailures shouldBe empty }
      withClue(s"should have saved exactly $parallelism entities") { saveFailures shouldBe empty }

      // query all entities for workspace (odd)
      val actualEntitiesOdd = runAndWait(entityQuery.listActiveEntitiesOfType(workspaceOdd, entityType))
      // query all entities for workspace (even)
      val actualEntitiesEven = runAndWait(entityQuery.listActiveEntitiesOfType(workspaceEven, entityType))

      // validate save counts entities
      val expectedCountOdd = Math.ceil(parallelism.toDouble/2d)
      val expectedCountEven = Math.floor(parallelism.toDouble/2d)
      withClue(s"should have saved exactly $expectedCountOdd and $expectedCountEven entities" +
        s" to workspaces odd and even, respectively") {
        (actualEntitiesOdd.size, actualEntitiesEven.size) shouldBe (expectedCountOdd, expectedCountEven)
      }

      // validate individual entities (odd)
      actualEntitiesOdd.foreach { actual =>
        // name should be the index
        val idx = actual.name.toInt
        val expected = entitiesToSave.get(idx).orElse(fail(s"an entity was saved with name $idx;" +
          s" that name should not exist"))
        withClue(s"entity with name ${actual.name} should have saved to the odd workspace") { isOdd(actual.name.toInt) }
        withClue(s"actual entity did not match expected") { Option(actual) shouldBe (expected) }
      }

      //validate that the entity saves happened in parallel by looking for
      //multiple unique thread ids
      assert(threadIds.values.toSet.size > 1)

      // validate individual entities (even)
      actualEntitiesEven.foreach { actual =>
        // name should be the index
        val idx = actual.name.toInt
        val expected = entitiesToSave.get(idx).orElse(fail(s"an entity was saved with name $idx;" +
          s" that name should not exist"))
        withClue(s"entity with name ${actual.name} should have saved to the even workspace") { isEven(actual.name.toInt) }
        withClue(s"actual entity did not match expected") { Option(actual) shouldBe (expected) }
      }
    }
  }

  it should "rewrite attributes" in withEmptyTestDatabase {
    def insertAndUpdateID(rec: WorkspaceAttributeRecord): WorkspaceAttributeRecord = {
      rec.copy(id = runAndWait((workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += rec))
    }

    runAndWait(workspaceQuery.save(workspace))

    val existing = Seq(
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId1, workspaceId, AttributeName.defaultNamespace, "test1", Option("test"), None, None, None, None, None, None, deleted = false, None)),
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test2", None, Option(2), None, None, None, None, None, deleted = false, None))
    )

    assertExpectedRecords(existing:_*)

    val update = WorkspaceAttributeRecord(2, workspaceId, AttributeName.defaultNamespace, "test2", Option("test2"), None, None, None, None, None, None, deleted = false, None)
    val insert = WorkspaceAttributeRecord(3, workspaceId, AttributeName.defaultNamespace, "test3", None, None, Option(false), None, None, None, None, deleted = false, None)
    val toSave = Seq(update, insert)

    runAndWait(workspaceAttributeQuery.rewriteAttrsAction(toSave, existing, workspaceAttributeTempQuery.insertScratchAttributes))

    assertExpectedRecords(toSave:_*)
  }

  it should "apply attribute patch" in withEmptyTestDatabase {
    def insertAndUpdateID(rec: WorkspaceAttributeRecord): WorkspaceAttributeRecord = {
      rec.copy(id = runAndWait((workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += rec))
    }

    runAndWait(workspaceQuery.save(workspace))

    val existing = Seq(
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId1, workspaceId, AttributeName.defaultNamespace, "test1", Option("test"), None, None, None, None, None, None, deleted = false, None)),
      insertAndUpdateID(WorkspaceAttributeRecord(dummyId2, workspaceId, AttributeName.defaultNamespace, "test2", None, Option(2), None, None, None, None, None, deleted = false, None))
    )

    assertExpectedRecords(existing:_*)

    val update = WorkspaceAttributeRecord(2, workspaceId, AttributeName.defaultNamespace, "test2", Option("test2"), None, None, None, None, None, None, deleted = false, None)
    val insert = WorkspaceAttributeRecord(3, workspaceId, AttributeName.defaultNamespace, "test3", None, None, Option(false), None, None, None, None, deleted = false, None)

    //test insert and update
    runAndWait(workspaceAttributeQuery.patchAttributesAction(Seq(insert), Seq(update), Seq(), workspaceAttributeTempQuery.insertScratchAttributes))
    assertExpectedRecords(Seq(existing.head, update, insert):_*)

    //test delete
    runAndWait(workspaceAttributeQuery.patchAttributesAction(Seq(), Seq(), existing.map(_.id), workspaceAttributeTempQuery.insertScratchAttributes))
    assertExpectedRecords(Seq(insert):_*)
  }

  it should "findUniqueStringsByNameQuery shouldn't return any duplicates, limit results by namespace" in withEmptyTestDatabase{

    runAndWait(workspaceQuery.save(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("testString"), AttributeString("cant")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cantaloupe")))


    val workspace2ID = UUID.randomUUID()
    val workspace2 = Workspace("broad-dsde-test", "test-tag-workspace", workspace2ID.toString, "fake-bucket", Some("workflow-collection"), DateTime.now, DateTime.now, "testuser", Map.empty, false)
    runAndWait(workspaceQuery.save(workspace2))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("buffalo")))

    assertResult(Vector(("cancer", 2), ("cantaloupe", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, Some("can")).result)
    }

    assertResult(Vector(("cancer", 2), ("buffalo", 1), ("cantaloupe", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, None).result)
    }
    assertResult(Vector(("cant", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withDefaultNS("testString"), Some("can")).result)
    }
  }

  private def runWorkspaceSaveNewTest(insertAttribute: Attribute, updateAttribute: Attribute): Unit = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { context =>
        // Try to insert new attribute
        val inserts = Map(AttributeName.withDefaultNS("newWorkspaceAttribute") -> insertAttribute)

        val expectedAfterInsertion = testData.workspace.attributes ++ inserts

        runAndWait(workspaceQuery.save(
          testData.workspace.copy(attributes = expectedAfterInsertion)
        ))

        val resultAfterInsert = runAndWait(workspaceQuery.findById(testData.workspace.workspaceId)).head.attributes

        assertSameElements(expectedAfterInsertion, resultAfterInsert)

        // Try to update the new attribute
        val updates: AttributeMap = Map(AttributeName.withDefaultNS("newWorkspaceAttribute") -> updateAttribute)

        val expectedAfterUpdate = testData.workspace.attributes ++ updates

        runAndWait(workspaceQuery.save(
          testData.workspace.copy(attributes = expectedAfterUpdate)
        ))

        val resultAfterUpdate = runAndWait(workspaceQuery.findById(testData.workspace.workspaceId)).head.attributes

        // check that the new attribute has been updated
        assertSameElements(expectedAfterUpdate, resultAfterUpdate)
      }
    }
  }

  private def runEntitySaveNewTest(insertAttribute: Attribute, updateAttribute: Attribute): Unit = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { context =>
        // Try to insert new attribute
        val inserts = Map(AttributeName.withDefaultNS("newEntityAttribute") -> insertAttribute)

        val expectedAfterInsertion = testData.sample1.attributes ++ inserts

        runAndWait(entityQuery.save(
          context,
          //testData.sample1.copy(attributes = expectedAfterInsertion),
          // We could just use the above... but why not instead use the wonderful constructor that uses
          // (name: String, type: String) instead of (type: String, name:String) like the lookup methods.
          // No. I did not spend a long time debugging after copy/paste/swapping the name and type by accident.
          // Why do you ask?
          Entity("sample1", "Sample", expectedAfterInsertion),
        ))

        val resultAfterInsert = runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes

        // check that the new attribute has been updated
        assertSameElements(expectedAfterInsertion, resultAfterInsert)

        // Try to update the new attribute
        val updates: AttributeMap = Map(AttributeName.withDefaultNS("newEntityAttribute") -> updateAttribute)

        val expectedAfterUpdate = testData.sample1.attributes ++ updates

        runAndWait(entityQuery.save(
          context,
          testData.sample1.copy(attributes = expectedAfterUpdate),
        ))

        val resultAfterUpdate = runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes

        // check that the new attribute has been updated
        assertSameElements(expectedAfterUpdate, resultAfterUpdate)
      }
    }
  }

  private def runEntityPatchNewTest(insertAttribute: Attribute, updateAttribute: Attribute): Unit = {
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { context =>
        // insert new attribute
        val inserts = Map(AttributeName.withDefaultNS("newEntityAttribute") -> insertAttribute)

        val expectedAfterInsertion = testData.sample1.attributes ++ inserts

        runAndWait(entityQuery.saveEntityPatch(
          context,
          testData.sample1.toReference,
          inserts,
          Seq.empty[AttributeName]
        ))

        val resultAfterInsert = runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes

        assertSameElements(expectedAfterInsertion, resultAfterInsert)

        // update the new attribute
        val updates: AttributeMap = Map(AttributeName.withDefaultNS("newEntityAttribute") -> updateAttribute)

        val expectedAfterUpdate = testData.sample1.attributes ++ updates

        runAndWait(entityQuery.saveEntityPatch(
          context,
          testData.sample1.toReference,
          updates,
          Seq.empty[AttributeName])
        )

        val resultAfterUpdate = runAndWait(entityQuery.get(context, "Sample", "sample1")).head.attributes

        // check that the new attribute has been updated
        assertSameElements(expectedAfterUpdate, resultAfterUpdate)
      }
    }
  }

  private case class AttributeTestFunction(description: String, run: (Attribute, Attribute) => Unit)

  private val attributeTestFunctions = List(
    AttributeTestFunction("workspaceQuery.save()", runWorkspaceSaveNewTest),
    AttributeTestFunction("entityQuery.save()", runEntitySaveNewTest),
    AttributeTestFunction("entityQuery.saveEntityPatch()", runEntityPatchNewTest),
  )

  private case class AttributeTestData(description: String, attribute: Attribute)

  private val attributeTestData = List(
    AttributeTestData(
      "a null attribute value",
      AttributeNull
    ),
    AttributeTestData(
      "an attribute single value",
      AttributeString("abc1"),
    ),
    AttributeTestData(
      "an attribute list with a single value",
      AttributeValueList(List(
        AttributeString("abc2"),
      )),
    ),
    AttributeTestData(
      "an attribute list of strings",
      AttributeValueList(List(
        AttributeString("abc3"),
        AttributeString("def"),
        AttributeString("abc12"),
        AttributeString("xyz"),
      )),
    ),
    AttributeTestData(
      "an attribute list of json",
      AttributeValueList(List(
        AttributeValueRawJson(JsObject("key1" -> JsString("valueA"))),
        AttributeValueRawJson(JsObject("key2" -> JsString("valueB"))),
        AttributeValueRawJson(JsObject("key3" -> JsString("valueC"))),
        AttributeValueRawJson(JsObject("key4" -> JsString("valueD"))),
      )),
    ),
  )

  attributeTestFunctions.foreach { attributeTestFunction =>
    // Test all combinations, including a->b AND the reverse b->a
    attributeTestData.combinations(2).flatMap(x => List(x, x.reverse)).foreach {
      case List(AttributeTestData(description1, attribute1), AttributeTestData(description2, attribute2)) =>
        it should s"reflect ${attributeTestFunction.description} changes in " +
          s"a new attribute when $description1 changes to $description2" in {
          attributeTestFunction.run(attribute1, attribute2)
        }
    }
  }

}
