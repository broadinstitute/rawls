package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceState, _}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.joda.time.DateTime
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import spray.json.{JsObject, JsString}

import java.util.UUID

/**
 * Created by dvoet on 2/9/16.
 */
//noinspection EmptyParenMethodAccessedAsParameterless,RedundantDefaultArgument,NameBooleanParameters,MutatorLikeMethodIsParameterless,ScalaUnnecessaryParentheses,ScalaUnusedSymbol,TypeAnnotation
class AttributeComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with AttributeComponent
    with RawlsTestUtils {
  import driver.api._

  val workspace = Workspace(
    "broad-dsde-test",
    "test-workspace",
    UUID.randomUUID().toString,
    "fake-bucket",
    Some("workflow-collection"),
    DateTime.now,
    DateTime.now,
    "biden",
    Map.empty,
    false
  )
  val workspaceId = UUID.fromString(workspace.workspaceId)

  // we don't know the IDs because it autoincrements
  val (dummyId1, dummyId2) = (12345, 67890)
  def assertExpectedRecords(expectedRecords: WorkspaceAttributeRecord*): Unit = {
    val records = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result) map {
      _.copy(id = dummyId1)
    }
    assertSameElements(records, expectedRecords map { _.copy(id = dummyId1) })
  }

  def insertWorkspaceAttributeRecords(workspaceId: UUID,
                                      attributeName: AttributeName,
                                      attribute: Attribute
  ): ReadWriteAction[Option[Int]] =
    entityQuery.findActiveEntityByWorkspace(workspaceId).result flatMap { entityRecords =>
      val entityRecordsMap = entityRecords.map(e => e.toReference -> e.id).toMap
      workspaceAttributeQuery ++= workspaceAttributeQuery.marshalAttribute(workspaceId,
                                                                           attributeName,
                                                                           attribute,
                                                                           entityRecordsMap
      )
    }

  "AttributeComponent" should "insert string attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               Option("test"),
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  "AttributeComponent" should "insert string attribute as json" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("\"thisshouldbelegitright\"")

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               Option("\"thisshouldbelegitright\""),
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert library-namespace attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeString("test")
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(
      insertWorkspaceAttributeRecords(workspaceId, AttributeName(AttributeName.libraryNamespace, "test"), testAttribute)
    )

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.libraryNamespace,
                               "test",
                               Option("test"),
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNumber(3.14159)
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Option(3.14159),
                               None,
                               None,
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert json number attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("95")

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               Option("95"),
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeBoolean(true)
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               Option(true),
                               None,
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert json boolean attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("true")

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               Option("true"),
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert attribute value list" in withEmptyTestDatabase {
    val testAttribute =
      AttributeValueList(Seq(AttributeNumber(9), AttributeNumber(8), AttributeNumber(7), AttributeNumber(6)))
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Option(9),
                               None,
                               None,
                               None,
                               Option(0),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Option(8),
                               None,
                               None,
                               None,
                               Option(1),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Option(7),
                               None,
                               None,
                               None,
                               Option(2),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Option(6),
                               None,
                               None,
                               None,
                               Option(3),
                               Option(4),
                               false,
                               None
      )
    )
  }

  "AttributeComponent" should "insert json number list attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[9,3]")

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               Option("[9,3]"),
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert json mixed list attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson("[\"foo\",\"bar\",true,54]")

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               Option("[\"foo\",\"bar\",true,54]"),
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert empty value list" in withEmptyTestDatabase {
    val testAttribute = AttributeValueEmptyList
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Some(-1.0),
                               None,
                               None,
                               None,
                               None,
                               Option(0),
                               false,
                               None
      )
    )
  }

  it should "insert empty ref list" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceEmptyList
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               Option(0),
                               false,
                               None
      )
    )
  }

  it should "save empty AttributeValueLists as AttributeValueEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeValueList(Seq())
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               Some(-1.0),
                               None,
                               None,
                               None,
                               None,
                               Option(0),
                               false,
                               None
      )
    )
  }

  it should "save empty AttributeEntityReferenceLists as AttributeEntityReferenceEmptyList" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(Seq())
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               Option(0),
                               false,
                               None
      )
    )
  }

  it should "insert null attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeNull
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert entity reference attribute" in withEmptyTestDatabase {
    runAndWait(
      workspaceQuery += WorkspaceRecord(
        "testns",
        "testname1",
        workspaceId,
        "bucket",
        Some("workflow-collection"),
        defaultTimeStamp,
        defaultTimeStamp,
        "me",
        false,
        0,
        WorkspaceVersions.V2.value,
        "gp",
        None,
        None,
        None,
        Option(defaultTimeStamp),
        WorkspaceType.RawlsWorkspace.toString,
        WorkspaceState.Ready.toString
      )
    )
    val entityId = runAndWait(
      (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0,
                                                                    "name",
                                                                    "type",
                                                                    workspaceId,
                                                                    0,
                                                                    deleted = false,
                                                                    None
      )
    )
    val testAttribute = AttributeEntityReference("type", "name")
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               Option(entityId),
                               None,
                               None,
                               false,
                               None
      )
    )
  }

  it should "insert entity reference attribute list" in withEmptyTestDatabase {
    runAndWait(
      workspaceQuery += WorkspaceRecord(
        "testns",
        "testname2",
        workspaceId,
        "bucket",
        Some("workflow-collection"),
        defaultTimeStamp,
        defaultTimeStamp,
        "me",
        false,
        0,
        WorkspaceVersions.V2.value,
        "gp",
        None,
        None,
        None,
        Option(defaultTimeStamp),
        WorkspaceType.RawlsWorkspace.toString,
        WorkspaceState.Ready.toString
      )
    )
    val entityId1 = runAndWait(
      (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0,
                                                                    "name1",
                                                                    "type",
                                                                    workspaceId,
                                                                    0,
                                                                    deleted = false,
                                                                    None
      )
    )
    val entityId2 = runAndWait(
      (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0,
                                                                    "name2",
                                                                    "type",
                                                                    workspaceId,
                                                                    0,
                                                                    deleted = false,
                                                                    None
      )
    )
    val entityId3 = runAndWait(
      (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0,
                                                                    "name3",
                                                                    "type",
                                                                    workspaceId,
                                                                    0,
                                                                    deleted = false,
                                                                    None
      )
    )
    val entityId4 = runAndWait(
      (entityQuery returning entityQuery.map(_.id)) += EntityRecord(0,
                                                                    "name4",
                                                                    "type",
                                                                    workspaceId,
                                                                    0,
                                                                    deleted = false,
                                                                    None
      )
    )

    val testAttribute = AttributeEntityReferenceList(
      Seq(
        AttributeEntityReference("type", "name1"),
        AttributeEntityReference("type", "name2"),
        AttributeEntityReference("type", "name3"),
        AttributeEntityReference("type", "name4")
      )
    )
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))

    assertExpectedRecords(
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               Option(entityId1),
                               Option(0),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               Option(entityId2),
                               Option(1),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               Option(entityId3),
                               Option(2),
                               Option(4),
                               false,
                               None
      ),
      WorkspaceAttributeRecord(dummyId2,
                               workspaceId,
                               AttributeName.defaultNamespace,
                               "test",
                               None,
                               None,
                               None,
                               None,
                               Option(entityId4),
                               Option(3),
                               Option(4),
                               false,
                               None
      )
    )

  }

  it should "throw exception inserting ref to nonexistent entity" in withEmptyTestDatabase {
    runAndWait(
      workspaceQuery += WorkspaceRecord(
        "testns",
        "testname3",
        workspaceId,
        "bucket",
        Some("workflow-collection"),
        defaultTimeStamp,
        defaultTimeStamp,
        "me",
        false,
        0,
        WorkspaceVersions.V2.value,
        "gp",
        None,
        None,
        None,
        Option(defaultTimeStamp),
        WorkspaceType.RawlsWorkspace.toString,
        WorkspaceState.Ready.toString
      )
    )
    val testAttribute = AttributeEntityReference("type", "name")
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "throw exception inserting inconsistent list values" in withEmptyTestDatabase {
    val testAttribute =
      AttributeValueList(Seq(AttributeNumber(9), AttributeString("oops"), AttributeNumber(7), AttributeNumber(6)))
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "throw exception inserting inconsistent list references" in withEmptyTestDatabase {
    val testAttribute = AttributeEntityReferenceList(
      Seq(AttributeEntityReference("type1", "foo"),
          AttributeEntityReference("type2", "foo"),
          AttributeEntityReference("type1", "foo")
      )
    )
    intercept[RawlsException] {
      runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    }
  }

  it should "insert json object attribute" in withEmptyTestDatabase {
    val testAttribute = AttributeValueRawJson(
      "{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}"
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("test"), testAttribute))
    assertExpectedRecords(
      WorkspaceAttributeRecord(
        dummyId2,
        workspaceId,
        AttributeName.defaultNamespace,
        "test",
        None,
        None,
        None,
        Option(
          "{\"field1\":5,\"field2\":false,\"field3\":\"hiiii\",\"field4\":{\"subfield1\":[4,true,\"ohno\"],\"subfield2\":false}}"
        ),
        None,
        None,
        None,
        false,
        None
      )
    )

  }

  it should "delete attribute records" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    val inserts = Seq(
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(
        dummyId2,
        workspaceId,
        AttributeName.defaultNamespace,
        "test1",
        None,
        Some(1),
        None,
        None,
        None,
        None,
        None,
        false,
        None
      ),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(
        dummyId2,
        workspaceId,
        AttributeName.defaultNamespace,
        "test2",
        None,
        Some(2),
        None,
        None,
        None,
        None,
        None,
        false,
        None
      ),
      (workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += WorkspaceAttributeRecord(
        dummyId2,
        workspaceId,
        AttributeName.defaultNamespace,
        "test3",
        None,
        Some(3),
        None,
        None,
        None,
        None,
        None,
        false,
        None
      )
    ) map { insert =>
      runAndWait(insert)
    }
    val attributeRecs = runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result)
    assertResult(3)(attributeRecs.size)

    assertResult(3)(runAndWait(workspaceAttributeQuery.deleteAttributeRecordsById(attributeRecs.map(_.id))))

    assertResult(0)(runAndWait(workspaceAttributeQuery.filter(_.ownerId === workspaceId).result).size)
  }

  it should "unmarshal attribute records" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "string",
                                 Some("value"),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.libraryNamespace,
                                 "string",
                                 Some("lib-value"),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "num",
                                 None,
                                 Some(1),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "bool",
                                 None,
                                 None,
                                 Some(true),
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "ref",
                                 None,
                                 None,
                                 None,
                                 None,
                                 Some(1),
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       Some(EntityRecord(0, "name", "type", workspaceId, 0, deleted = false, None))
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "null",
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((2,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "valList",
                                 None,
                                 Some(1),
                                 None,
                                 None,
                                 None,
                                 Some(2),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       None
      ),
      ((2,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "valList",
                                 None,
                                 Some(2),
                                 None,
                                 None,
                                 None,
                                 Some(1),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       None
      ),
      ((2,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "valList",
                                 None,
                                 Some(3),
                                 None,
                                 None,
                                 None,
                                 Some(0),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "refList",
                                 None,
                                 None,
                                 None,
                                 None,
                                 Some(1),
                                 Some(2),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       Some(EntityRecord(0, "name1", "type", workspaceId, 0, deleted = false, None))
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "refList",
                                 None,
                                 None,
                                 None,
                                 None,
                                 Some(2),
                                 Some(1),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       Some(EntityRecord(0, "name2", "type", workspaceId, 0, deleted = false, None))
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "refList",
                                 None,
                                 None,
                                 None,
                                 None,
                                 Some(3),
                                 Some(0),
                                 Some(3),
                                 false,
                                 None
        )
       ),
       Some(EntityRecord(0, "name3", "type", workspaceId, 0, deleted = false, None))
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "emptyList",
                                 None,
                                 Some(1),
                                 None,
                                 None,
                                 None,
                                 Some(-1),
                                 Some(0),
                                 false,
                                 None
        )
       ),
       None
      )
    )

    assertResult(
      Map(
        1 -> Map(
          AttributeName.withDefaultNS("string") -> AttributeString("value"),
          AttributeName(AttributeName.libraryNamespace, "string") -> AttributeString("lib-value"),
          AttributeName.withDefaultNS("num") -> AttributeNumber(1),
          AttributeName.withDefaultNS("bool") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("ref") -> AttributeEntityReference("type", "name"),
          AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
            Seq(AttributeEntityReference("type", "name3"),
                AttributeEntityReference("type", "name2"),
                AttributeEntityReference("type", "name1")
            )
          ),
          AttributeName.withDefaultNS("emptyList") -> AttributeValueEmptyList,
          AttributeName.withDefaultNS("null") -> AttributeNull
        ),
        2 -> Map(
          AttributeName.withDefaultNS("valList") -> AttributeValueList(
            Seq(AttributeNumber(3), AttributeNumber(2), AttributeNumber(1))
          )
        )
      )
    ) {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)

    }
  }

  it should "unmarshal ints and doubles with appropriate precision" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "int",
                                 None,
                                 Some(2),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "negInt",
                                 None,
                                 Some(-2),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "doubleRounded",
                                 None,
                                 Some(2.0),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "negDoubleRounded",
                                 None,
                                 Some(-2.0),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "double",
                                 None,
                                 Some(2.34),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      ),
      ((1,
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "negDouble",
                                 None,
                                 Some(-2.34),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 false,
                                 None
        )
       ),
       None
      )
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
        )
      )
    )(unmarshalled)

    val attrs = unmarshalled(1)

    def assertScale(key: String, expectedScale: Int) =
      attrs(AttributeName.withDefaultNS(key)) match {
        case num: AttributeNumber => assertResult(expectedScale, s"scale of $key attribute")(num.value.scale)
        case _                    => fail(s"expected AttributeNumber for $key")
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
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(1),
                                     None,
                                     None,
                                     None,
                                     Some(2),
                                     Some(3),
                                     false,
                                     None
       ),
       None
      ),
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(2),
                                     None,
                                     None,
                                     None,
                                     Some(1),
                                     None,
                                     false,
                                     None
       ),
       None
      ),
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(3),
                                     None,
                                     None,
                                     None,
                                     Some(0),
                                     Some(3),
                                     false,
                                     None
       ),
       None
      )
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "throw exception unmarshalling a list without listIndex set for all" in withEmptyTestDatabase {
    val attributeRecs = Seq(
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(1),
                                     None,
                                     None,
                                     None,
                                     Some(2),
                                     Some(3),
                                     false,
                                     None
       ),
       None
      ),
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(2),
                                     None,
                                     None,
                                     None,
                                     None,
                                     Some(3),
                                     false,
                                     None
       ),
       None
      ),
      (1 -> WorkspaceAttributeRecord(dummyId2,
                                     workspaceId,
                                     AttributeName.defaultNamespace,
                                     "valList",
                                     None,
                                     Some(3),
                                     None,
                                     None,
                                     None,
                                     Some(0),
                                     Some(3),
                                     false,
                                     None
       ),
       None
      )
    )

    intercept[RawlsException] {
      workspaceAttributeQuery.unmarshalAttributes(attributeRecs)
    }
  }

  it should "rewrite attributes" in withEmptyTestDatabase {
    def insertAndUpdateID(rec: WorkspaceAttributeRecord): WorkspaceAttributeRecord =
      rec.copy(id = runAndWait((workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += rec))

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val existing = Seq(
      insertAndUpdateID(
        WorkspaceAttributeRecord(dummyId1,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "test1",
                                 Option("test"),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 deleted = false,
                                 None
        )
      ),
      insertAndUpdateID(
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "test2",
                                 None,
                                 Option(2),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 deleted = false,
                                 None
        )
      )
    )

    assertExpectedRecords(existing: _*)

    val update = WorkspaceAttributeRecord(2,
                                          workspaceId,
                                          AttributeName.defaultNamespace,
                                          "test2",
                                          Option("test2"),
                                          None,
                                          None,
                                          None,
                                          None,
                                          None,
                                          None,
                                          deleted = false,
                                          None
    )
    val insert = WorkspaceAttributeRecord(3,
                                          workspaceId,
                                          AttributeName.defaultNamespace,
                                          "test3",
                                          None,
                                          None,
                                          Option(false),
                                          None,
                                          None,
                                          None,
                                          None,
                                          deleted = false,
                                          None
    )
    val toSave = Seq(update, insert)

    runAndWait(
      workspaceAttributeQuery.rewriteAttrsAction(toSave, existing, workspaceAttributeTempQuery.insertScratchAttributes)
    )

    assertExpectedRecords(toSave: _*)
  }

  it should "apply attribute patch" in withEmptyTestDatabase {
    def insertAndUpdateID(rec: WorkspaceAttributeRecord): WorkspaceAttributeRecord =
      rec.copy(id = runAndWait((workspaceAttributeQuery returning workspaceAttributeQuery.map(_.id)) += rec))

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val existing = Seq(
      insertAndUpdateID(
        WorkspaceAttributeRecord(dummyId1,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "test1",
                                 Option("test"),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 deleted = false,
                                 None
        )
      ),
      insertAndUpdateID(
        WorkspaceAttributeRecord(dummyId2,
                                 workspaceId,
                                 AttributeName.defaultNamespace,
                                 "test2",
                                 None,
                                 Option(2),
                                 None,
                                 None,
                                 None,
                                 None,
                                 None,
                                 deleted = false,
                                 None
        )
      )
    )

    assertExpectedRecords(existing: _*)

    val update = WorkspaceAttributeRecord(2,
                                          workspaceId,
                                          AttributeName.defaultNamespace,
                                          "test2",
                                          Option("test2"),
                                          None,
                                          None,
                                          None,
                                          None,
                                          None,
                                          None,
                                          deleted = false,
                                          None
    )
    val insert = WorkspaceAttributeRecord(3,
                                          workspaceId,
                                          AttributeName.defaultNamespace,
                                          "test3",
                                          None,
                                          None,
                                          Option(false),
                                          None,
                                          None,
                                          None,
                                          None,
                                          deleted = false,
                                          None
    )

    // test insert and update
    runAndWait(
      workspaceAttributeQuery.patchAttributesAction(Seq(insert),
                                                    Seq(update),
                                                    Seq(),
                                                    workspaceAttributeTempQuery.insertScratchAttributes,
                                                    RawlsTracingContext(None)
      )
    )
    assertExpectedRecords(Seq(existing.head, update, insert): _*)

    // test delete
    runAndWait(
      workspaceAttributeQuery.patchAttributesAction(Seq(),
                                                    Seq(),
                                                    existing.map(_.id),
                                                    workspaceAttributeTempQuery.insertScratchAttributes,
                                                    RawlsTracingContext(None)
      )
    )
    assertExpectedRecords(Seq(insert): _*)
  }

  it should "findUniqueStringsByNameQuery shouldn't return any duplicates, limit results by namespace" in withEmptyTestDatabase {

    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(
      insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("testString"), AttributeString("cant"))
    )
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("Buffalo")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cantaloupe")))

    val workspace2ID = UUID.randomUUID()
    val workspace2 = Workspace(
      "broad-dsde-test",
      "test-tag-workspace",
      workspace2ID.toString,
      "fake-bucket",
      Some("workflow-collection"),
      DateTime.now,
      DateTime.now,
      "testuser",
      Map.empty,
      false
    )
    runAndWait(workspaceQuery.createOrUpdate(workspace2))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("buffalo")))

    assertResult(Vector(("cancer", 2), ("cantaloupe", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, Some("can")).result)
    }

    assertResult(Vector(("cancer", 2), ("Buffalo", 1), ("buffalo", 1), ("cantaloupe", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, None).result)
    }
    assertResult(Vector(("cancer", 2), ("Buffalo", 1))) {
      runAndWait(workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, None, Some(2)).result)
    }
    assertResult(Vector(("cant", 1))) {
      runAndWait(
        workspaceAttributeQuery
          .findUniqueStringsByNameQuery(AttributeName.withDefaultNS("testString"), Some("can"))
          .result
      )
    }
  }

  it should "findUniqueStringsByNameQuery should only return tags from the specified workspaceIds" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    runAndWait(
      insertWorkspaceAttributeRecords(workspaceId, AttributeName.withDefaultNS("testString"), AttributeString("cant"))
    )
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("Buffalo")))
    runAndWait(insertWorkspaceAttributeRecords(workspaceId, AttributeName.withTagsNS, AttributeString("cantaloupe")))

    val workspace2ID = UUID.randomUUID()
    val workspace2 = Workspace(
      "broad-dsde-test",
      "test-tag-workspace",
      workspace2ID.toString,
      "fake-bucket",
      Some("workflow-collection"),
      DateTime.now,
      DateTime.now,
      "testuser",
      Map.empty,
      false
    )
    runAndWait(workspaceQuery.createOrUpdate(workspace2))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("cancer")))
    runAndWait(insertWorkspaceAttributeRecords(workspace2ID, AttributeName.withTagsNS, AttributeString("buffalo")))

    assertResult(Vector(("cancer", 1), ("cantaloupe", 1))) {
      runAndWait(
        workspaceAttributeQuery
          .findUniqueStringsByNameQuery(AttributeName.withTagsNS,
                                        queryString = Some("can"),
                                        limit = None,
                                        ownerIds = Some(Seq(workspaceId))
          )
          .result
      )
    }

    assertResult(Vector(("cancer", 2), ("cantaloupe", 1))) {
      runAndWait(
        workspaceAttributeQuery
          .findUniqueStringsByNameQuery(AttributeName.withTagsNS,
                                        queryString = Some("can"),
                                        limit = None,
                                        ownerIds = Some(Seq(workspaceId, workspace2ID))
          )
          .result
      )
    }
  }

  private def runWorkspaceSaveNewTest(insertAttribute: Attribute, updateAttribute: Attribute): Unit =
    withDefaultTestDatabase {
      withWorkspaceContext(testData.workspace) { context =>
        // Try to insert new attribute
        val inserts = Map(AttributeName.withDefaultNS("newWorkspaceAttribute") -> insertAttribute)

        val expectedAfterInsertion = testData.workspace.attributes ++ inserts

        runAndWait(
          workspaceQuery.createOrUpdate(
            testData.workspace.copy(attributes = expectedAfterInsertion)
          )
        )

        val resultAfterInsert =
          runAndWait(workspaceQuery.findById(testData.workspace.workspaceId)).head.attributes

        assertSameElements(expectedAfterInsertion, resultAfterInsert)

        // Try to update the new attribute
        val updates: AttributeMap = Map(AttributeName.withDefaultNS("newWorkspaceAttribute") -> updateAttribute)

        val expectedAfterUpdate = testData.workspace.attributes ++ updates

        runAndWait(
          workspaceQuery.createOrUpdate(
            testData.workspace.copy(attributes = expectedAfterUpdate)
          )
        )

        val resultAfterUpdate =
          runAndWait(workspaceQuery.findById(testData.workspace.workspaceId)).head.attributes

        // check that the new attribute has been updated
        assertSameElements(expectedAfterUpdate, resultAfterUpdate)
      }
    }

  private case class AttributeTestFunction(description: String, run: (Attribute, Attribute) => Unit)

  private val attributeTestFunctions = List(
    AttributeTestFunction("workspaceQuery.createOrUpdate()", runWorkspaceSaveNewTest)
  )

  private case class AttributeTestData(description: String, attribute: Attribute)

  private val attributeTestData = List(
    AttributeTestData(
      "a null attribute value",
      AttributeNull
    ),
    AttributeTestData(
      "an attribute single value",
      AttributeString("abc1")
    ),
    AttributeTestData(
      "an attribute list with a single value",
      AttributeValueList(
        List(
          AttributeString("abc2")
        )
      )
    ),
    AttributeTestData(
      "an attribute list of strings",
      AttributeValueList(
        List(
          AttributeString("abc3"),
          AttributeString("def"),
          AttributeString("abc12"),
          AttributeString("xyz")
        )
      )
    ),
    AttributeTestData(
      "an attribute list of json",
      AttributeValueList(
        List(
          AttributeValueRawJson(JsObject("key1" -> JsString("valueA"))),
          AttributeValueRawJson(JsObject("key2" -> JsString("valueB"))),
          AttributeValueRawJson(JsObject("key3" -> JsString("valueC"))),
          AttributeValueRawJson(JsObject("key4" -> JsString("valueD")))
        )
      )
    )
  )

  attributeTestFunctions.foreach { attributeTestFunction =>
    // Test all combinations, including a->b AND the reverse b->a
    attributeTestData.combinations(2).flatMap(x => List(x, x.reverse)).foreach {
      case List(AttributeTestData(description1, attribute1), AttributeTestData(description2, attribute2)) =>
        it should s"reflect ${attributeTestFunction.description} changes in " +
          s"a new attribute when $description1 changes to $description2" in
          attributeTestFunction.run(attribute1, attribute2)
      case x =>
        throw new Exception(s"${x} is unexpected")
    }
  }

  // workspaceAttributeQuery is a Scala object, whose final class Mockito cannot spy; this non-final equivalent can be spied
  class SpyableWorkspaceAttributeQuery
      extends AttributeQuery[UUID, WorkspaceAttributeRecord, WorkspaceAttributeTable](new WorkspaceAttributeTable(_),
                                                                                      WorkspaceAttributeRecord
      )

  it should "skip inserts, updates, and deletes when rewriting workspace attributes if nothing changed" in withEmptyTestDatabase {
    // no need to save workspace; this test should not write anything
    val baseRec = WorkspaceAttributeRecord(0,
                                           workspaceId,
                                           "default",
                                           "name1",
                                           Some("strValue"),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           false,
                                           None
    )

    val existingAttributes = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1")),
      baseRec.copy(id = 2, name = "attr2", valueString = None, valueNumber = Some(2)),
      baseRec.copy(id = 3, name = "attr3", valueString = None, valueBoolean = Some(true))
    )

    val spiedAttrQuery = spy(new SpyableWorkspaceAttributeQuery)

    runAndWait(
      spiedAttrQuery.rewriteAttrsAction(existingAttributes,
                                        existingAttributes,
                                        workspaceAttributeTempQuery.insertScratchAttributes
      )
    )

    val insertsCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val updatesCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val deletesCaptor: ArgumentCaptor[Traversable[Long]] = ArgumentCaptor.forClass(classOf[Traversable[Long]])

    verify(spiedAttrQuery, times(1)).patchAttributesAction(insertsCaptor.capture(),
                                                           updatesCaptor.capture(),
                                                           deletesCaptor.capture(),
                                                           any(),
                                                           any()
    )

    assert(insertsCaptor.getValue.isEmpty, "inserts should be empty")
    assert(updatesCaptor.getValue.isEmpty, "updates should be empty")
    assert(deletesCaptor.getValue.isEmpty, "deletes should be empty")
  }

  it should "skip inserts and deletes when rewriting workspace attributes if only updating" in withEmptyTestDatabase {
    // save workspace to use as parent
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val baseRec = WorkspaceAttributeRecord(0,
                                           workspaceId,
                                           "default",
                                           "name1",
                                           Some("strValue"),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           false,
                                           None
    )

    val existingAttributes = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1")),
      baseRec.copy(id = 2, name = "attr2", valueString = None, valueNumber = Some(2)),
      baseRec.copy(id = 3, name = "attr3", valueString = None, valueBoolean = Some(true))
    )

    val updates = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1 UPDATED"))
    )

    val attributesToSave = updates ++ existingAttributes.tail

    val insertsCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val updatesCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val deletesCaptor: ArgumentCaptor[Traversable[Long]] = ArgumentCaptor.forClass(classOf[Traversable[Long]])

    val spiedAttrQuery = spy(new SpyableWorkspaceAttributeQuery)

    runAndWait(
      spiedAttrQuery.rewriteAttrsAction(attributesToSave,
                                        existingAttributes,
                                        workspaceAttributeTempQuery.insertScratchAttributes
      )
    )

    verify(spiedAttrQuery, times(1)).patchAttributesAction(insertsCaptor.capture(),
                                                           updatesCaptor.capture(),
                                                           deletesCaptor.capture(),
                                                           any(),
                                                           any()
    )

    assert(insertsCaptor.getValue.isEmpty, "insertsCaptor should be empty")
    withClue("should have one update")(assertSameElements(updates, updatesCaptor.getValue))
    assert(deletesCaptor.getValue.isEmpty, "deletes should be empty")
  }

  it should "skip updates and deletes when rewriting workspace attributes if only inserting" in withEmptyTestDatabase {
    // save workspace to use as parent
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val baseRec = WorkspaceAttributeRecord(0,
                                           workspaceId,
                                           "default",
                                           "name1",
                                           Some("strValue"),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           false,
                                           None
    )

    val existingAttributes = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1")),
      baseRec.copy(id = 2, name = "attr2", valueString = None, valueNumber = Some(2)),
      baseRec.copy(id = 3, name = "attr3", valueString = None, valueBoolean = Some(true))
    )

    val inserts = Seq(
      baseRec.copy(id = 4, name = "attr4", valueString = Some("this is a new attr"))
    )

    val attributesToSave = inserts ++ existingAttributes

    val insertsCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val updatesCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val deletesCaptor: ArgumentCaptor[Traversable[Long]] = ArgumentCaptor.forClass(classOf[Traversable[Long]])

    val spiedAttrQuery = spy(new SpyableWorkspaceAttributeQuery)

    runAndWait(
      spiedAttrQuery.rewriteAttrsAction(attributesToSave,
                                        existingAttributes,
                                        workspaceAttributeTempQuery.insertScratchAttributes
      )
    )

    verify(spiedAttrQuery, times(1)).patchAttributesAction(insertsCaptor.capture(),
                                                           updatesCaptor.capture(),
                                                           deletesCaptor.capture(),
                                                           any(),
                                                           any()
    )

    withClue("should have one insert")(assertSameElements(inserts, insertsCaptor.getValue))
    assert(updatesCaptor.getValue.isEmpty, "updates should be empty")
    assert(deletesCaptor.getValue.isEmpty, "deletes should be empty")
  }

  it should "skip inserts and updates when rewriting workspace attributes if only deleting" in withEmptyTestDatabase {
    // save workspace to use as parent
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val baseRec = WorkspaceAttributeRecord(0,
                                           workspaceId,
                                           "default",
                                           "name1",
                                           Some("strValue"),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           false,
                                           None
    )

    val existingAttributes = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1")),
      baseRec.copy(id = 2, name = "attr2", valueString = None, valueNumber = Some(2)),
      baseRec.copy(id = 3, name = "attr3", valueString = None, valueBoolean = Some(true))
    )

    val attributesToSave = existingAttributes.tail // <-- attr1 should be deleted

    val insertsCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val updatesCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val deletesCaptor: ArgumentCaptor[Traversable[Long]] = ArgumentCaptor.forClass(classOf[Traversable[Long]])

    val spiedAttrQuery = spy(new SpyableWorkspaceAttributeQuery)

    runAndWait(
      spiedAttrQuery.rewriteAttrsAction(attributesToSave,
                                        existingAttributes,
                                        workspaceAttributeTempQuery.insertScratchAttributes
      )
    )

    verify(spiedAttrQuery, times(1)).patchAttributesAction(insertsCaptor.capture(),
                                                           updatesCaptor.capture(),
                                                           deletesCaptor.capture(),
                                                           any(),
                                                           any()
    )

    assert(insertsCaptor.getValue.isEmpty, "inserts should be empty")
    assert(updatesCaptor.getValue.isEmpty, "updates should be empty")
    withClue("should have one delete")(assertSameElements(Seq(1), deletesCaptor.getValue))
  }

  it should "combine inserts, updates, and deletes when rewriting workspace attributes" in withEmptyTestDatabase {
    // save workspace to use as parent
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val baseRec = WorkspaceAttributeRecord(0,
                                           workspaceId,
                                           "default",
                                           "name1",
                                           Some("strValue"),
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           false,
                                           None
    )

    val existingAttributes = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1")),
      baseRec.copy(id = 2, name = "attr2", valueString = None, valueNumber = Some(2)),
      baseRec.copy(id = 3, name = "attr3", valueString = None, valueBoolean = Some(true))
    )

    val updates = Seq(
      baseRec.copy(id = 1, name = "attr1", valueString = Some("value1 UPDATED"))
    )

    val inserts = Seq(
      baseRec.copy(id = 4, name = "attr4", valueString = Some("this is a new attr"))
    )

    val attributesToSave = updates ++ inserts // <-- attr2 and attr3 should be deleted

    val insertsCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val updatesCaptor: ArgumentCaptor[Traversable[WorkspaceAttributeRecord]] =
      ArgumentCaptor.forClass(classOf[Traversable[WorkspaceAttributeRecord]])
    val deletesCaptor: ArgumentCaptor[Traversable[Long]] = ArgumentCaptor.forClass(classOf[Traversable[Long]])

    val spiedAttrQuery = spy(new SpyableWorkspaceAttributeQuery)

    runAndWait(
      spiedAttrQuery.rewriteAttrsAction(attributesToSave,
                                        existingAttributes,
                                        workspaceAttributeTempQuery.insertScratchAttributes
      )
    )

    verify(spiedAttrQuery, times(1)).patchAttributesAction(insertsCaptor.capture(),
                                                           updatesCaptor.capture(),
                                                           deletesCaptor.capture(),
                                                           any(),
                                                           any()
    )

    withClue("should have one insert")(assertSameElements(inserts, insertsCaptor.getValue))
    withClue("should have one update")(assertSameElements(updates, updatesCaptor.getValue))
    withClue("should have two deletes")(assertSameElements(Seq(2, 3), deletesCaptor.getValue))
  }

}
