package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValue,
  AttributeValueList,
  Entity,
  EntityColumnFilter,
  EntityQuery,
  FilterOperators,
  SortDirections,
  WorkspaceFieldSpecs
}
import slick.dbio.Effect.Read
import slick.sql.SqlStreamingAction
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.SQLIntegrityConstraintViolationException
import java.util.UUID
import scala.util.Random

class CompactEntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val q = compactEntityQuery

  behavior of "batchCreateEntities and getEntity"

  // tests both batchCreateEntities and getEntity
  it should "handle entities with no attributes" in withMinimalTestDatabase { _ =>
    val entities = Seq(
      Entity("entityName1", "entityType1", Map()),
      Entity("entityName2", "entityType2", Map()),
      Entity("entityName3", "entityType3", Map())
    )
    insertAndGetAll(entities)
  }

  it should "handle entities with simple attributes" in withMinimalTestDatabase { _ =>
    val entities = Seq(
      Entity("entityName1",
             "entityType1",
             Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
      ),
      Entity("entityName2",
             "entityType2",
             Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
      ),
      Entity("entityName3",
             "entityType3",
             Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
      )
    )
    insertAndGetAll(entities)
  }

  // TODO CORE-428: behavior of this test will change once batch updates are implemented
  it should "make no db changes if any of the entities exist" in withMinimalTestDatabase { _ =>
    val entity1 = Entity("entityName1", "entityType", Map())
    val entity2 = Entity("entityName2", "entityType", Map())
    val entity3 = Entity("entityName3", "entityType", Map())

    insertAndGet(entity2)

    val entities = Seq(entity1, entity2, entity3)

    // should throw a primary key violation error
    intercept[SQLIntegrityConstraintViolationException](runAndWait(q.batchCreateEntities(wsid, entities)))

    // entity 2 should still exist
    val rec2 = runAndWait(q.getEntity(wsid, entity2.entityType, entity2.name))
    rec2 shouldBe defined
    rec2.get.toEntity shouldBe entity2

    // entities 1 and 3 should not exist
    runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name)) shouldBe empty
    runAndWait(q.getEntity(wsid, entity3.entityType, entity3.name)) shouldBe empty
  }

  // TODO CORE-428: behavior of this test will change once batch updates are implemented
  it should "make no db changes if input contains repeated entities" in withMinimalTestDatabase { _ =>
    val entities = Seq(
      Entity("entityName1", "entityType1", Map()),
      Entity("entityName2", "entityType2", Map()),
      Entity("entityName1", "entityType1", Map()) // duplicate of the first entity
    )

    // should throw a primary key violation error
    intercept[SQLIntegrityConstraintViolationException](insertAndGetAll(entities))

    runAndWait(q.getEntity(wsid, "entityType1", "entityName1")) shouldBe empty
    runAndWait(q.getEntity(wsid, "entityType2", "entityName2")) shouldBe empty
  }

  behavior of "createEntity and getEntity"

  // tests both createEntity and getEntity
  it should "handle an entity with no attributes" in withMinimalTestDatabase { _ =>
    val entity = Entity("entityName", "entityType", Map())
    insertAndGet(entity)
  }

  it should "handle an entity with simple attributes" in withMinimalTestDatabase { _ =>
    val entity = Entity("entityName",
                        "entityType",
                        Map(
                          AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                          AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                        )
    )
    insertAndGet(entity)
  }
  it should "handle an entity with references" in withMinimalTestDatabase { _ =>
    val targetType = "target"
    val entity = Entity(
      "entityName",
      "entityType",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "1"),
        AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference(targetType, "2"),
            AttributeEntityReference(targetType, "3")
          )
        )
      )
    )
    insertAndGet(entity)
  }

  behavior of "getEntityRefs"

  it should "return nothing if asked for nothing" in withMinimalTestDatabase { _ =>
    val entity1 = Entity("entityName1", "entityType", Map())
    val entity2 = Entity("entityName2", "entityType", Map())
    val entity3 = Entity("entityName3", "entityType", Map())

    insertAndGetAll(Seq(entity1, entity2, entity3))

    val actual = runAndWait(q.getEntityRefs(wsid, Set.empty))
    actual shouldBe empty
  }

  it should "return existent entities" in withMinimalTestDatabase { _ =>
    val entity1 = Entity("entityName1", "entityType", Map())
    val entity2 = Entity("entityName2", "entityType", Map())
    val entity3 = Entity("entityName3", "entityType", Map())

    insertAndGetAll(Seq(entity1, entity2, entity3))

    val actual = runAndWait(q.getEntityRefs(wsid, Set(entity1.toReference, entity2.toReference)))

    val expected = Set(entity1.toReference, entity2.toReference)

    // map CompactEntityRefRecord to AttributeEntityReference when comparing, since
    // CompactEntityRefRecord contains an id which can be different every time
    actual.map(_.toAttributeEntityReference) should contain theSameElementsAs expected
  }

  it should "return what it found even if not all entities exist" in withMinimalTestDatabase { _ =>
    val entity1 = Entity("entityName1", "entityType", Map())
    val entity2 = Entity("entityName2", "entityType", Map())
    val entity3 = Entity("entityName3", "entityType", Map())

    insertAndGetAll(Seq(entity1, entity2, entity3))

    val actual = runAndWait(
      q.getEntityRefs(wsid,
                      Set(entity1.toReference,
                          AttributeEntityReference(entity2.entityType, "nonexistent-2"),
                          entity2.toReference
                      )
      )
    )
    val expected = Set(entity1.toReference, entity2.toReference)

    // map CompactEntityRefRecord to AttributeEntityReference when comparing, since
    // CompactEntityRefRecord contains an id which can be different every time
    actual.map(_.toAttributeEntityReference) should contain theSameElementsAs expected
  }

  behavior of "ENTITY_KEYS population triggers"
  // the ENTITY_KEYS table is populated by triggers on the ENTITY table.
  // these tests verify the behavior of those triggers.

  it should "save a row with empty keys for an entity with no attributes" in withMinimalTestDatabase { _ =>
    val entity = Entity("entityName", "entityType", Map())
    val savedEntity = insertAndGet(entity)
    // get the keys
    val actual = runAndWait(q.getKeys(savedEntity.id))
    actual should not be empty
    actual.get.attributeKeys shouldBe "[]"
  }

  it should "save keys for an entity with attributes" in withMinimalTestDatabase { _ =>
    val attributeNames = List("red", "green", "blue", "pfb:importedColumn")
    // define an entity with attributes named according to ${attributeNames}
    val entity = Entity(
      "entityName",
      "entityType",
      attributeNames.map { attrName =>
        AttributeName.fromDelimitedName(attrName) -> AttributeNumber(System.currentTimeMillis())
      }.toMap
    )
    val savedEntity = insertAndGet(entity)
    // get the keys
    val actual = runAndWait(q.getKeys(savedEntity.id))
    actual should not be empty
    actual.get.attributeKeys.parseJson.convertTo[List[String]] should contain theSameElementsAs attributeNames
  }

  behavior of "getReferencedIds(workspaceId, Set[AttributeEntityReference]"

  it should "return the found references" in withMinimalTestDatabase { _ =>
    // insert the rows being referenced
    val targetType = "target"
    val target1 = insertAndGet(Entity("target1", targetType, Map()))
    val target2 = insertAndGet(Entity("target2", targetType, Map()))
    val target3 = insertAndGet(Entity("target3", targetType, Map()))

    val refs: Set[AttributeEntityReference] = Set(
      AttributeEntityReference(targetType, "target1"),
      AttributeEntityReference(targetType, "target2"),
      AttributeEntityReference(targetType, "target3")
    )

    val actual = runAndWait(q.getReferencedIds(wsid, refs))

    actual should contain theSameElementsAs List(target1.id, target2.id, target3.id)
  }

  it should "return nothing if references are not found" in withMinimalTestDatabase { _ =>
    // insert some rows to ensure they are NOT returned
    val targetType = "target"
    insertAndGet(Entity("target1", targetType, Map()))
    insertAndGet(Entity("target2", targetType, Map()))
    insertAndGet(Entity("target3", targetType, Map()))

    val refs: Set[AttributeEntityReference] = Set(
      AttributeEntityReference(targetType, "nonexistent-1"),
      AttributeEntityReference(targetType, "nonexistent-2")
    )

    val actual = runAndWait(q.getReferencedIds(wsid, refs))

    actual shouldBe empty
  }

  behavior of "upsertReferences and deleteReferences"

  it should "insert and delete all" in withMinimalTestDatabase { _ =>
    val fromId: Long = 1 // id of the entity doing the referencing: the "source"
    val toIds: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
    // insert rows
    runAndWait(q.upsertReferences(Set(RefPointers(fromId, toIds)))) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIds
    // delete rows
    runAndWait(q.deleteReferencesWithFilter(fromId, Set())) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
  }

  it should "insert and delete partial" in withMinimalTestDatabase { _ =>
    val fromId: Long = 1 // id of the entity doing the referencing: the "source"
    val toIds: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
    // insert rows
    runAndWait(q.upsertReferences(Set(RefPointers(fromId, toIds)))) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIds
    // delete rows, keeping the first two from toIds
    val toKeep = toIds.take(2)
    runAndWait(q.deleteReferencesWithFilter(fromId, toKeep)) shouldBe toIds.size - toKeep.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toKeep
  }

  it should "insert and delete non-overlapping" in withMinimalTestDatabase { _ =>
    val fromId: Long = 1 // id of the entity doing the referencing: the "source"
    val toIdsOne: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    val toIdsTwo: Set[Long] = Set(201, 202, 203) // ids of entities being referenced: the "targets"
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
    // insert rows for set one
    runAndWait(q.upsertReferences(Set(RefPointers(fromId, toIdsOne)))) shouldBe toIdsOne.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIdsOne
    // delete rows, specifying to keep those in set two (which has no overlap with set one)
    runAndWait(q.deleteReferencesWithFilter(fromId, toIdsTwo)) shouldBe toIdsOne.size
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
  }

  it should "upsert for multiple source entities" in withMinimalTestDatabase { _ =>
    val fromIdOne: Long = 1 // id of the entity doing the referencing: the "source"
    val fromIdTwo: Long = 2 // id of the entity doing the referencing: the "source"
    val toIdsOne: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    val toIdsTwo: Set[Long] = Set(201, 202, 203, 104) // notice the overlap for target 104
    // sources should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromIdOne)) shouldBe empty
    runAndWait(q.getReferencedIds(fromIdTwo)) shouldBe empty
    // insert
    runAndWait(
      q.upsertReferences(Set(RefPointers(fromIdOne, toIdsOne), RefPointers(fromIdTwo, toIdsTwo)))
    ) shouldBe toIdsOne.size + toIdsTwo.size
    runAndWait(q.getReferencedIds(fromIdOne)) should contain theSameElementsAs toIdsOne
    runAndWait(q.getReferencedIds(fromIdTwo)) should contain theSameElementsAs toIdsTwo
  }

  behavior of "listEntityKeys"

  it should "return the keys for a workspace" in withMinimalTestDatabase { _ =>
    // create 2 entity types with different attributes
    val entityType1AttributeNames =
      List("red", "green", "blue", "orange", "yellow", "purple").map(AttributeName.withDefaultNS)
    val entityType1 = "entityType1"
    createEntitiesWithKeys(entityType1AttributeNames, entityType1, wsid)

    val entityType2AttributeNames =
      List("circle", "square", "triangle", "rectangle", "oval", "hexagon").map(AttributeName.withDefaultNS)
    val entityType2 = "entityType2"
    createEntitiesWithKeys(entityType2AttributeNames, entityType2, wsid)

    // create an entity with no attributes to make sure it does not break anything
    insertAndGet(
      Entity(
        UUID.randomUUID().toString,
        "noAttributes",
        Map.empty
      )
    )

    // create entities in a different workspace to make sure they are not included
    createEntitiesWithKeys(entityType1AttributeNames, entityType2, minimalTestData.workspace2.workspaceIdAsUUID)

    val actual = runAndWait(q.listEntityKeys(wsid))
    actual should contain theSameElementsAs entityType1AttributeNames.map {
      EntityTypeAndAttributeKey(entityType1, _)
    } ++ entityType2AttributeNames.map {
      EntityTypeAndAttributeKey(entityType2, _)
    }
  }

  /**
   * Creates 1 entity with the first half of keys, 1 entity with the second half of keys, and 1 entity with no keys.
   */
  private def createEntitiesWithKeys(entityType1AttributeNames: List[AttributeName],
                                     entityType1: String,
                                     workspaceId: UUID
  ): Unit = {
    val half = entityType1AttributeNames.size / 2
    insertAndGet(
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        entityType1AttributeNames.take(half).map(_ -> AttributeNumber(System.currentTimeMillis())).toMap
      ),
      workspaceId
    )
    insertAndGet(
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        entityType1AttributeNames.drop(half).map(_ -> AttributeNumber(System.currentTimeMillis())).toMap
      ),
      workspaceId
    )
    insertAndGet(
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map.empty
      ),
      workspaceId
    )
  }

  behavior of "countEntitiesGroupedByType"

  it should "return the count of entities grouped by type" in withMinimalTestDatabase { _ =>
    // insert an entity with attributes
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    // get the count of entities grouped by type
    val actual = runAndWait(q.countEntitiesGroupedByType(wsid))
    actual should contain theSameElementsAs List(EntityTypeAndCount(entityType1, 2), EntityTypeAndCount(entityType2, 1))
  }

  private val columnFilterCases = List(
    (AttributeNumber(42), "42"),
    (AttributeString("foo"), "foo"),
    (AttributeString("fOo"), "foO") // case sensitivity check
  )

  behavior of "countEntitiesWithColumnFilter"

  columnFilterCases.foreach { case (attrValue, filterValue) =>
    it should s"return the count of entities with filter $attrValue" in withMinimalTestDatabase { _ =>
      val entityType1 = "entityType1"
      val entityType2 = "entityType2"
      val testAttrName = AttributeName.withDefaultNS("foo")
      val entity1 =
        Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName -> attrValue))
      val entity2 =
        Entity(UUID.randomUUID().toString, entityType2, Map(testAttrName -> attrValue))
      val entity3 =
        Entity(UUID.randomUUID().toString,
               entityType1,
               Map(testAttrName -> AttributeString(UUID.randomUUID().toString))
        )
      val entity4 =
        Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName -> attrValue))
      insertAndGet(entity1) // should get counted
      insertAndGet(entity2) // different entityType
      insertAndGet(entity3) // different attribute value
      insertAndGet(entity4) // should get counted
      insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

      // get the count of entities grouped by type
      val actual = runAndWait(
        q.countEntitiesWithColumnFilter(wsid, entityType1, EntityColumnFilter(testAttrName, filterValue))
      )
      actual shouldBe 2
    }
  }

  behavior of "queryEntitiesWithColumnFilter"

  columnFilterCases.foreach { case (attrValue, filterValue) =>
    it should s"return the entities with filter $attrValue sorted by name" in withMinimalTestDatabase { _ =>
      val entityType1 = "entityType1"
      val entityType2 = "entityType2"
      val testAttrName = AttributeName.withDefaultNS("foo")
      val entity1 =
        Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName -> attrValue))
      val entity2 =
        Entity(UUID.randomUUID().toString, entityType2, Map(testAttrName -> attrValue))
      val entity3 =
        Entity(UUID.randomUUID().toString,
               entityType1,
               Map(testAttrName -> AttributeString(UUID.randomUUID().toString))
        )
      val entity4 =
        Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName -> attrValue))
      insertAndGet(entity1) // should get counted
      insertAndGet(entity2) // different entityType
      insertAndGet(entity3) // different attribute value
      insertAndGet(entity4) // should get counted
      insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

      val columnFilter = EntityColumnFilter(testAttrName, filterValue)
      val actual = runAndWait(
        q.queryEntitiesWithColumnFilter(
          wsid,
          entityType1,
          EntityQuery(1,
                      10,
                      Attributable.nameReservedAttribute,
                      SortDirections.Ascending,
                      None,
                      columnFilter = Some(columnFilter)
          ),
          columnFilter
        )
      )
      actual should contain theSameElementsInOrderAs List(entity1, entity4).sortBy(_.name)
    }
  }

  columnFilterCases.foreach { case (attrValue, filterValue) =>
    it should s"return the entities with filter $attrValue sorted by attribute" in withMinimalTestDatabase { _ =>
      logger.debug("hello world")
      val entityType1 = "entityType1"
      val entityType2 = "entityType2"
      val testAttrName = AttributeName.withDefaultNS("foo")
      val sortAttrName = AttributeName.withDefaultNS("sortMe")
      val entity1 =
        Entity(UUID.randomUUID().toString,
               entityType1,
               Map(testAttrName -> attrValue, sortAttrName -> AttributeNumber(Random.nextInt()))
        )
      val entity2 =
        Entity(UUID.randomUUID().toString,
               entityType2,
               Map(testAttrName -> attrValue, sortAttrName -> AttributeNumber(Random.nextInt()))
        )
      val entity3 =
        Entity(
          UUID.randomUUID().toString,
          entityType1,
          Map(testAttrName -> AttributeString(UUID.randomUUID().toString),
              sortAttrName -> AttributeNumber(Random.nextInt())
          )
        )
      val entity4 =
        Entity(UUID.randomUUID().toString,
               entityType1,
               Map(testAttrName -> attrValue, sortAttrName -> AttributeNumber(Random.nextInt()))
        )
      insertAndGet(entity1) // should get counted
      insertAndGet(entity2) // different entityType
      insertAndGet(entity3) // different attribute value
      insertAndGet(entity4) // should get counted
      insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

      val columnFilter = EntityColumnFilter(testAttrName, filterValue)
      val actual = runAndWait(
        q.queryEntitiesWithColumnFilter(
          wsid,
          entityType1,
          EntityQuery(1,
                      10,
                      toDelimitedName(sortAttrName),
                      SortDirections.Ascending,
                      None,
                      columnFilter = Some(columnFilter)
          ),
          columnFilter
        )
      )
      actual should contain theSameElementsInOrderAs List(entity1, entity4).sortBy(
        _.attributes(sortAttrName).asInstanceOf[AttributeNumber].value
      )
    }
  }

  it should "sort by attribute list size" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val testAttrName = AttributeName.withDefaultNS("foo")
    val sortAttrName = AttributeName.withDefaultNS("sortMe")
    val entity1 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(testAttrName -> AttributeString("foo"),
            sortAttrName -> AttributeValueList(List.fill(7)(AttributeNumber(Random.nextInt())))
        )
      )
    val entity2 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(testAttrName -> AttributeString("foo"),
            sortAttrName -> AttributeValueList(List.fill(3)(AttributeNumber(Random.nextInt())))
        )
      )
    val entity3 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(testAttrName -> AttributeString("foo"),
            sortAttrName -> AttributeValueList(List.fill(9)(AttributeNumber(Random.nextInt())))
        )
      )
    val entity4 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName -> AttributeString("foo"), sortAttrName -> AttributeNumber(Random.nextInt()))
      )
    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)
    insertAndGet(entity4) // this one does not have a list so should have a sort value of 1

    val columnFilter = EntityColumnFilter(testAttrName, "foo")
    val actual = runAndWait(
      q.queryEntitiesWithColumnFilter(
        wsid,
        entityType1,
        EntityQuery(1,
                    10,
                    toDelimitedName(sortAttrName),
                    SortDirections.Ascending,
                    None,
                    columnFilter = Some(columnFilter)
        ),
        columnFilter
      )
    )
    actual should contain theSameElementsInOrderAs List(entity4, entity2, entity1, entity3)
  }

  it should "respect desired fields" in withMinimalTestDatabase { _ =>
    val columnFilter = EntityColumnFilter(AttributeName.withDefaultNS("foo"), "foo")
    testDesiredFields(None, Some(columnFilter)) { (entityType, entityQuery) =>
      q.queryEntitiesWithColumnFilter(
        wsid,
        entityType,
        entityQuery,
        columnFilter
      )
    }
  }

  behavior of "queryEntitiesWithFilterTerms"

  it should "return the entities with filter terms FilterOperators.And sorted by name" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val testAttrName1 = AttributeName.withDefaultNS("foo")
    val testAttrName2 = AttributeName.withDefaultNS("bar")
    val entity1 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString("asdffoo"), testAttrName2 -> AttributeString("bar"))
      )
    val entity2 =
      Entity(UUID.randomUUID().toString,
             entityType2,
             Map(testAttrName1 -> AttributeString("foo"), testAttrName2 -> AttributeString("bar"))
      )
    val entity3 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString(UUID.randomUUID().toString), testAttrName2 -> AttributeString("bar"))
      )
    val entity4 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString("foo"), testAttrName2 -> AttributeString("barasdf"))
      )
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // different attribute value
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.queryEntitiesWithFilterTerms(
        wsid,
        entityType1,
        EntityQuery(1,
                    10,
                    Attributable.nameReservedAttribute,
                    SortDirections.Ascending,
                    Some("foo bAr"),
                    FilterOperators.And
        )
      )
    )
    actual should contain theSameElementsInOrderAs List(entity1, entity4).sortBy(_.name)
  }

  it should "return the entities with filter terms FilterOperators.Or sorted by name" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val testAttrName1 = AttributeName.withDefaultNS("foo")
    val entity1 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString("asdffoo")))
    val entity2 =
      Entity(UUID.randomUUID().toString, entityType2, Map(testAttrName1 -> AttributeString("foo")))
    val entity3 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString(UUID.randomUUID().toString)))
    val entity4 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString("barasdf")))
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // different attribute value
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.queryEntitiesWithFilterTerms(
        wsid,
        entityType1,
        EntityQuery(1,
                    10,
                    Attributable.nameReservedAttribute,
                    SortDirections.Ascending,
                    Some("foo bAr"),
                    FilterOperators.Or
        )
      )
    )
    actual should contain theSameElementsInOrderAs List(entity1, entity4).sortBy(_.name)
  }

  it should "return the entities with filter terms FilterOperators.And sorted by attribute" in withMinimalTestDatabase {
    _ =>
      val entityType1 = "entityType1"
      val entityType2 = "entityType2"
      val testAttrName1 = AttributeName.withDefaultNS("foo")
      val testAttrName2 = AttributeName.withDefaultNS("bar")
      val sortAttrName = AttributeName.withDefaultNS("sortMe")
      val entity1 =
        Entity(
          UUID.randomUUID().toString,
          entityType1,
          Map(testAttrName1 -> AttributeString("asdffoo"),
              testAttrName2 -> AttributeString("bar"),
              sortAttrName -> AttributeNumber(Random.nextInt())
          )
        )
      val entity2 =
        Entity(
          UUID.randomUUID().toString,
          entityType2,
          Map(testAttrName1 -> AttributeString("foo"),
              testAttrName2 -> AttributeString("bar"),
              sortAttrName -> AttributeNumber(Random.nextInt())
          )
        )
      val entity3 =
        Entity(
          UUID.randomUUID().toString,
          entityType1,
          Map(testAttrName1 -> AttributeString(UUID.randomUUID().toString),
              testAttrName2 -> AttributeString("bar"),
              sortAttrName -> AttributeNumber(Random.nextInt())
          )
        )
      val entity4 =
        Entity(
          UUID.randomUUID().toString,
          entityType1,
          Map(testAttrName1 -> AttributeString("foo"),
              testAttrName2 -> AttributeString("barasdf"),
              sortAttrName -> AttributeNumber(Random.nextInt())
          )
        )
      insertAndGet(entity1) // should get counted
      insertAndGet(entity2) // different entityType
      insertAndGet(entity3) // different attribute value
      insertAndGet(entity4) // should get counted
      insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

      val actual = runAndWait(
        q.queryEntitiesWithFilterTerms(
          wsid,
          entityType1,
          EntityQuery(1,
                      10,
                      toDelimitedName(sortAttrName),
                      SortDirections.Ascending,
                      Some("foo bAr"),
                      FilterOperators.And
          )
        )
      )
      actual should contain theSameElementsInOrderAs List(entity1, entity4).sortBy(
        _.attributes(sortAttrName).asInstanceOf[AttributeNumber].value
      )
  }

  it should "respect desired fields" in withMinimalTestDatabase { _ =>
    testDesiredFields(Some("foo"), None) { (entityType, entityQuery) =>
      q.queryEntitiesWithFilterTerms(
        wsid,
        entityType,
        entityQuery
      )
    }
  }

  behavior of "countEntitiesWithFilterTerms"

  it should "return the count of entities with filter terms FilterOperators.And" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val testAttrName1 = AttributeName.withDefaultNS("foo")
    val testAttrName2 = AttributeName.withDefaultNS("bar")
    val entity1 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString("asdffoo"), testAttrName2 -> AttributeString("bar"))
      )
    val entity2 =
      Entity(UUID.randomUUID().toString,
             entityType2,
             Map(testAttrName1 -> AttributeString("foo"), testAttrName2 -> AttributeString("bar"))
      )
    val entity3 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString(UUID.randomUUID().toString), testAttrName2 -> AttributeString("bar"))
      )
    val entity4 =
      Entity(UUID.randomUUID().toString,
             entityType1,
             Map(testAttrName1 -> AttributeString("foo"), testAttrName2 -> AttributeString("barasdf"))
      )
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // different attribute value
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.countEntitiesWithFilterTerms(
        wsid,
        entityType1,
        EntityQuery(1,
                    10,
                    Attributable.nameReservedAttribute,
                    SortDirections.Ascending,
                    Some("foo bAr"),
                    FilterOperators.And
        )
      )
    )
    actual shouldBe 2
  }

  it should "return the count of entities with filter terms FilterOperators.Or" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val testAttrName1 = AttributeName.withDefaultNS("foo")
    val entity1 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString("asdffoo")))
    val entity2 =
      Entity(UUID.randomUUID().toString, entityType2, Map(testAttrName1 -> AttributeString("foo")))
    val entity3 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString(UUID.randomUUID().toString)))
    val entity4 =
      Entity(UUID.randomUUID().toString, entityType1, Map(testAttrName1 -> AttributeString("barasdf")))
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // different attribute value
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.countEntitiesWithFilterTerms(
        wsid,
        entityType1,
        EntityQuery(1,
                    10,
                    Attributable.nameReservedAttribute,
                    SortDirections.Ascending,
                    Some("foo bAr"),
                    FilterOperators.Or
        )
      )
    )
    actual shouldBe 2
  }

  behavior of "countEntities"

  it should "return the count of entities" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)
    insertAndGet(entity4) // different workspace

    val actual = runAndWait(q.countEntities(wsid, entityType1))
    actual shouldBe 3
  }

  behavior of "queryEntitiesWithNoFilter"

  it should "return the entities with no filter sorted by name" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 =
      Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 =
      Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 =
      Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 =
      Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // should get counted
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.queryEntitiesWithNoFilter(
        wsid,
        entityType1,
        EntityQuery(1, 10, Attributable.nameReservedAttribute, SortDirections.Ascending, None)
      )
    )
    actual should contain theSameElementsInOrderAs List(entity1, entity3, entity4).sortBy(_.name)
  }

  it should "return the entities with no filter sorted by attribute" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val sortAttrName = AttributeName.withDefaultNS("sortMe")
    val entity1 =
      Entity(UUID.randomUUID().toString, entityType1, Map(sortAttrName -> AttributeNumber(Random.nextInt())))
    val entity2 =
      Entity(UUID.randomUUID().toString, entityType2, Map(sortAttrName -> AttributeNumber(Random.nextInt())))
    val entity3 =
      Entity(UUID.randomUUID().toString, entityType1, Map(sortAttrName -> AttributeNumber(Random.nextInt())))
    val entity4 =
      Entity(UUID.randomUUID().toString, entityType1, Map(sortAttrName -> AttributeNumber(Random.nextInt())))
    insertAndGet(entity1) // should get counted
    insertAndGet(entity2) // different entityType
    insertAndGet(entity3) // should get counted
    insertAndGet(entity4) // should get counted
    insertAndGet(entity4, minimalTestData.workspace2.workspaceIdAsUUID) // different workspace

    val actual = runAndWait(
      q.queryEntitiesWithNoFilter(
        wsid,
        entityType1,
        EntityQuery(1, 10, toDelimitedName(sortAttrName), SortDirections.Ascending, None)
      )
    )
    actual should contain theSameElementsInOrderAs List(entity1, entity3, entity4).sortBy(
      _.attributes(sortAttrName).asInstanceOf[AttributeNumber].value
    )
  }

  it should "return the entities with no filter sorted by list attribute" in withMinimalTestDatabase { _ =>
    val entityType1 = "entityType1"
    val sortAttrName = AttributeName.withDefaultNS("sortMe")
    val entity1 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(sortAttrName -> AttributeValueList(List.fill(7)(AttributeNumber(Random.nextInt()))))
      )
    val entity2 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(sortAttrName -> AttributeValueList(List.fill(3)(AttributeNumber(Random.nextInt()))))
      )
    val entity3 =
      Entity(
        UUID.randomUUID().toString,
        entityType1,
        Map(sortAttrName -> AttributeValueList(List.fill(9)(AttributeNumber(Random.nextInt()))))
      )
    val entity4 =
      Entity(UUID.randomUUID().toString, entityType1, Map(sortAttrName -> AttributeNumber(Random.nextInt())))
    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)
    insertAndGet(entity4) // this one does not have a list so should have a sort value of 1

    val actual = runAndWait(
      q.queryEntitiesWithNoFilter(
        wsid,
        entityType1,
        EntityQuery(1, 10, toDelimitedName(sortAttrName), SortDirections.Ascending, None)
      )
    )
    actual should contain theSameElementsInOrderAs List(entity4, entity2, entity1, entity3)
  }

  it should "respect desired fields" in withMinimalTestDatabase { _ =>
    testDesiredFields(None, None) { (entityType, entityQuery) =>
      q.queryEntitiesWithNoFilter(
        wsid,
        entityType,
        entityQuery
      )
    }
  }

  // ====================================================================================================
  //  helpers for tests
  // ====================================================================================================

  // this does NOT delegate to insertAndGetAll. This helper uses createEntity.
  private def insertAndGet(entity: Entity, workspaceId: UUID = wsid): CompactEntityRecord = {
    // row should not exist before inserting
    runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name)) shouldBe empty

    // insert the entities
    runAndWait(q.createEntity(workspaceId, entity)) shouldBe 1
    // retrieve the entities; retrieved value includes its id

    val actual = runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name))
    actual should not be empty
    val rec = actual.get
    // check entityType, name, and attributes
    rec.toEntity shouldBe entity
    // check other db columns which are not present in the Entity object
    rec.deleted shouldBe false
    rec.recordVersion shouldBe 0

    rec
  }

  // This helper uses batchCreateEntities.
  private def insertAndGetAll(entities: Seq[Entity], workspaceId: UUID = wsid): Seq[CompactEntityRecord] = {
    // rows should not exist before inserting
    entities.foreach { entity =>
      runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name)) shouldBe empty
    }

    // insert the entities
    runAndWait(q.batchCreateEntities(workspaceId, entities)) shouldBe entities.size
    // retrieve the entities; retrieved value includes its id
    entities.map { entity =>
      val actual = runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name))
      actual should not be empty
      val rec = actual.get
      // check entityType, name, and attributes
      rec.toEntity shouldBe entity
      // check other db columns which are not present in the Entity object
      rec.deleted shouldBe false
      rec.recordVersion shouldBe 0

      rec
    }
  }

  def testDesiredFields(filterTerms: Option[String], columnFilter: Option[EntityColumnFilter])(
    testQuery: (String, EntityQuery) => SqlStreamingAction[Seq[Entity], Entity, Read]
  ): Unit = {
    val entityType = "entityType"
    val columnFilterAttr = AttributeName.withDefaultNS("foo")
    val desiredColumnAttr1 = AttributeName.withDefaultNS("bar")
    val desiredColumnAttr2 = AttributeName.withDefaultNS("baz")
    val entity1 =
      Entity(UUID.randomUUID().toString,
             entityType,
             Map(columnFilterAttr -> AttributeString("foo"), desiredColumnAttr1 -> AttributeString("bar"))
      )
    val entity2 =
      Entity(
        UUID.randomUUID().toString,
        entityType,
        Map(columnFilterAttr -> AttributeString("foo"),
            desiredColumnAttr2 -> AttributeValueList(Seq(AttributeString("baz"), AttributeString("qux")))
        )
      )
    insertAndGet(entity1)
    insertAndGet(entity2)

    val entityQuery = EntityQuery(
      1,
      10,
      Attributable.nameReservedAttribute,
      SortDirections.Ascending,
      filterTerms,
      columnFilter = columnFilter,
      fields = WorkspaceFieldSpecs(Some(Set(toDelimitedName(desiredColumnAttr1), toDelimitedName(desiredColumnAttr2))))
    )
    val actual = runAndWait(testQuery(entityType, entityQuery))

    actual should contain theSameElementsAs List(
      entity1.copy(attributes = Map(desiredColumnAttr1 -> AttributeString("bar"), desiredColumnAttr2 -> AttributeNull)),
      entity2.copy(attributes =
        Map(desiredColumnAttr1 -> AttributeNull,
            desiredColumnAttr2 -> AttributeValueList(Seq(AttributeString("baz"), AttributeString("qux")))
        )
      )
    )
  }
}
