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

  behavior of "batchCreateEntities, getEntity and listEntities"

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
    intercept[SQLIntegrityConstraintViolationException](
      runAndWait(q.batchCreateEntities(wsid, entities, allowUpsert = false))
    )

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

  behavior of "deleteAllReferencesFrom"

  it should "delete references for multiple entities" in withMinimalTestDatabase { _ =>
    // referencing/source entities
    val sourceType1 = "source1"
    val sourceType2 = "source2"
    val source1 = insertAndGet(Entity("source1", sourceType1, Map()))
    val source2 = insertAndGet(Entity("source2", sourceType1, Map()))
    val source3 = insertAndGet(Entity("source3", sourceType2, Map()))
    // referenced/target entities
    val targetType = "target"
    val target1 = insertAndGet(Entity("target1", targetType, Map()))
    val target2 = insertAndGet(Entity("target2", targetType, Map()))
    val target3 = insertAndGet(Entity("target3", targetType, Map()))
    val target4 = insertAndGet(Entity("target4", targetType, Map()))
    val target5 = insertAndGet(Entity("target5", targetType, Map()))
    val target6 = insertAndGet(Entity("target6", targetType, Map()))
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(source1.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source2.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source3.id)) shouldBe empty
    val toIds1: Set[Long] = Set(target1.id, target2.id)
    val toIds2: Set[Long] = Set(target3.id, target4.id, target5.id)
    val toIds3: Set[Long] = Set(target1.id, target3.id, target6.id)
    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(source1.id, toIds1), RefPointers(source2.id, toIds2), RefPointers(source3.id, toIds3))
      )
    ) shouldBe (toIds1.size + toIds2.size + toIds3.size)
    runAndWait(q.getReferencedIds(source1.id)) should contain theSameElementsAs toIds1
    runAndWait(q.getReferencedIds(source2.id)) should contain theSameElementsAs toIds2
    runAndWait(q.getReferencedIds(source3.id)) should contain theSameElementsAs toIds3
    // delete rows
    runAndWait(
      q.deleteAllReferencesFrom(
        wsid,
        Set(
          AttributeEntityReference(sourceType1, source1.name),
          AttributeEntityReference(sourceType1, source2.name),
          AttributeEntityReference(sourceType2, source3.name)
        )
      )
    ) shouldBe (toIds1.size + toIds2.size + toIds3.size)
    runAndWait(q.getReferencedIds(source1.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source2.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source3.id)) shouldBe empty
  }

  it should "only delete references in the given workspace" in withMinimalTestDatabase { _ =>
    // referencing/source entities
    val sourceType = "source"
    val sourceWorkspace1 = insertAndGet(Entity("source", sourceType, Map()))
    val sourceWorkspace2 =
      insertAndGet(Entity("source", sourceType, Map()), minimalTestData.workspace2.workspaceIdAsUUID)
    // referenced/target entities
    val targetType = "target"
    val targetWorkspace1 = insertAndGet(Entity("target", targetType, Map()))
    val targetWorkspace2 =
      insertAndGet(Entity("target", targetType, Map()), minimalTestData.workspace2.workspaceIdAsUUID)
    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(sourceWorkspace1.id, Set(targetWorkspace1.id)),
            RefPointers(sourceWorkspace2.id, Set(targetWorkspace2.id))
        )
      )
    )
    runAndWait(q.getReferencedIds(sourceWorkspace1.id)) should contain theSameElementsAs Seq(targetWorkspace1.id)
    runAndWait(q.getReferencedIds(sourceWorkspace2.id)) should contain theSameElementsAs Seq(targetWorkspace2.id)
    // delete rows
    runAndWait(
      q.deleteAllReferencesFrom(
        wsid,
        Set(
          AttributeEntityReference(sourceType, sourceWorkspace1.name)
        )
      )
    ) shouldBe 1
    runAndWait(q.getReferencedIds(sourceWorkspace1.id)) shouldBe empty
    runAndWait(q.getReferencedIds(sourceWorkspace2.id)) should contain theSameElementsAs Seq(targetWorkspace2.id)
  }

  behavior of "deleteAllReferencesFromType"

  it should "only delete references for the given type" in withMinimalTestDatabase { _ =>
    // referencing/source entities
    val sourceType1 = "source1"
    val sourceType2 = "source2"
    val source1 = insertAndGet(Entity("source1", sourceType1, Map()))
    val source2 = insertAndGet(Entity("source2", sourceType1, Map()))
    val source3 = insertAndGet(Entity("source3", sourceType2, Map()))
    // referenced/target entities
    val targetType = "target"
    val target1 = insertAndGet(Entity("target1", targetType, Map()))
    val target2 = insertAndGet(Entity("target2", targetType, Map()))
    val target3 = insertAndGet(Entity("target3", targetType, Map()))
    val target4 = insertAndGet(Entity("target4", targetType, Map()))
    val target5 = insertAndGet(Entity("target5", targetType, Map()))
    val target6 = insertAndGet(Entity("target6", targetType, Map()))
    // source should have no rows in ENTITY_REFS table
    val toIds1: Set[Long] = Set(target1.id, target2.id)
    val toIds2: Set[Long] = Set(target3.id, target4.id, target5.id)
    val toIds3: Set[Long] = Set(target1.id, target3.id, target6.id)
    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(source1.id, toIds1), RefPointers(source2.id, toIds2), RefPointers(source3.id, toIds3))
      )
    ) shouldBe (toIds1.size + toIds2.size + toIds3.size)
    runAndWait(q.getReferencedIds(source1.id)) should contain theSameElementsAs toIds1
    runAndWait(q.getReferencedIds(source2.id)) should contain theSameElementsAs toIds2
    runAndWait(q.getReferencedIds(source3.id)) should contain theSameElementsAs toIds3
    // delete rows
    runAndWait(
      q.deleteAllReferencesFromType(
        wsid,
        sourceType1
      )
    ) shouldBe (toIds1.size + toIds2.size)
    runAndWait(q.getReferencedIds(source1.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source2.id)) shouldBe empty
    runAndWait(q.getReferencedIds(source3.id)) should not be empty
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

  behavior of "batchHide"

  it should "mark the entities as deleted and remove attributes" in withMinimalTestDatabase { _ =>
    // create entities to "delete"
    val entity1 = Entity("entityName1",
                         "entityType1",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                         )
    )
    val entity2 = Entity(
      "entityName2",
      "entityType2",
      Map(
        AttributeName.withDefaultNS("hello") -> AttributeString("world"),
        AttributeName.withDefaultNS("num") -> AttributeNumber(1)
      )
    )
    insertAndGet(entity1)
    insertAndGet(entity2)

    runAndWait(q.batchHide(wsid, Seq(entity1.toReference, entity2.toReference)))
    val actual1 = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    actual1 shouldBe empty
    val actual2 = runAndWait(q.getEntity(wsid, entity2.entityType, entity2.name))
    actual2 shouldBe empty

    val hidden1 = runAndWait(q.getDeletedEntity(wsid, entity1.entityType, entity1.name))
    hidden1 should not be empty
    hidden1.get.attributes shouldBe empty

    val hidden2 = runAndWait(q.getDeletedEntity(wsid, entity2.entityType, entity2.name))
    hidden2 should not be empty
    hidden2.get.attributes shouldBe empty

  }

  it should "not affect entities in other workspaces" in withMinimalTestDatabase { _ =>
    // create the entity to "delete"
    val entity = Entity("entityName",
                        "entityType",
                        Map(
                          AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                          AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                        )
    )
    insertAndGet(entity)

    // create a similar entity in another workspace
    insertAndGet(entity, minimalTestData.workspace2.workspaceIdAsUUID)

    runAndWait(q.batchHide(wsid, Seq(entity.toReference)))
    val actual = runAndWait(q.getEntity(wsid, entity.entityType, entity.name))
    actual shouldBe empty

    val twin = runAndWait(q.getEntity(minimalTestData.workspace2.workspaceIdAsUUID, entity.entityType, entity.name))
    twin should not be empty
    twin.get.attributes should not be empty
    twin.get.name shouldBe entity.name

  }

  behavior of "batchHideType"

  it should "only hide entities of the given type" in withMinimalTestDatabase { _ =>
    // create entities to "delete"
    val entity1 = Entity("entityName1",
                         "entityType1",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                         )
    )
    val entity2 = Entity(
      "entityName2",
      "entityType2",
      Map(
        AttributeName.withDefaultNS("hello") -> AttributeString("world"),
        AttributeName.withDefaultNS("num") -> AttributeNumber(1)
      )
    )
    val entity3 = Entity(
      "entityName3",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bah"),
        AttributeName.withDefaultNS("baz") -> AttributeNumber(24)
      )
    )
    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)

    runAndWait(q.batchHideType(wsid, entity1.entityType))
    val actual1 = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    actual1 shouldBe empty
    val actual2 = runAndWait(q.getEntity(wsid, entity2.entityType, entity2.name))
    actual2 should not be empty
    actual2.get.attributes should not be empty
    val actual3 = runAndWait(q.getEntity(wsid, entity3.entityType, entity3.name))
    actual3 shouldBe empty

    val hidden1 = runAndWait(q.getDeletedEntity(wsid, entity1.entityType, entity1.name))
    hidden1 should not be empty
    hidden1.get.attributes shouldBe empty

    val hidden3 = runAndWait(q.getDeletedEntity(wsid, entity3.entityType, entity3.name))
    hidden3 should not be empty
    hidden3.get.attributes shouldBe empty

  }

  behavior of "getReferencesTo"

  it should "find entities" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType1 = "targetType1"
    val targetType2 = "targetType2"
    val targetEntity1 = Entity("target1", targetType1, Map())
    val targetEntity2 = Entity("target2", targetType1, Map())
    val targetEntity3 = Entity("target3", targetType2, Map())
    val target1 = insertAndGet(targetEntity1)
    val target2 = insertAndGet(targetEntity2)
    val target3 = insertAndGet(targetEntity3)

    // create referencing/source entities
    val sourceEntity1 = Entity(
      "entity1",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType2, "target3")
      )
    )
    val sourceEntity2 =
      Entity("entity2",
             "entityType1",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target2"))
      )
    val sourceEntity3 =
      Entity("entity3",
             "entityType2",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"))
      )
    val entity1 = insertAndGet(sourceEntity1)
    val entity2 = insertAndGet(sourceEntity2)
    val entity3 = insertAndGet(sourceEntity3)

    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(entity1.id, Set(target1.id, target3.id)),
            RefPointers(entity2.id, Set(target2.id)),
            RefPointers(entity3.id, Set(target1.id))
        )
      )
    )

    val expected = Set(sourceEntity1.toReference, sourceEntity2.toReference, sourceEntity3.toReference)
    runAndWait(
      q.getReferencesTo(wsid, Seq(targetEntity1.toReference, targetEntity2.toReference))
    ) should contain theSameElementsAs expected

  }

  it should "not find entities in other workspaces" in withMinimalTestDatabase { _ =>
    // create referenced/target entity
    val targetType = "targetType"
    val targetEntity = Entity("target1", targetType, Map())
    val targetWorkspace1 = insertAndGet(targetEntity)
    val targetWorkspace2 = insertAndGet(targetEntity, minimalTestData.workspace2.workspaceIdAsUUID)

    // create referencing/source entities
    val sourceEntity = Entity(
      "source",
      "entityType",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "target1")
      )
    )
    val entityWorkspace1 = insertAndGet(sourceEntity)
    val entityWorkspace2 = insertAndGet(sourceEntity, minimalTestData.workspace2.workspaceIdAsUUID)

    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(entityWorkspace1.id, Set(targetWorkspace1.id)),
            RefPointers(entityWorkspace2.id, Set(targetWorkspace2.id))
        )
      )
    )

    val expected = Set(sourceEntity.toReference)
    runAndWait(
      q.getReferencesTo(wsid, Seq(targetEntity.toReference))
    ) should contain theSameElementsAs expected

  }

  it should "exclude entities included in the search" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType1 = "targetType1"
    val targetType2 = "targetType2"
    val targetEntity1 = Entity("target1", targetType1, Map())
    val targetEntity2 = Entity("target2", targetType1, Map())
    val targetEntity3 =
      Entity("target3",
             targetType2,
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"))
      )
    val target1 = insertAndGet(targetEntity1)
    val target2 = insertAndGet(targetEntity2)
    val target3 = insertAndGet(targetEntity3)

    // create referencing/source entities
    val sourceEntity1 = Entity(
      "entity1",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target2"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType2, "target3")
      )
    )
    val sourceEntity2 =
      Entity("entity2",
             "entityType1",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target2"))
      )
    val entity1 = insertAndGet(sourceEntity1)
    val entity2 = insertAndGet(sourceEntity2)

    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(entity1.id, Set(target3.id, target2.id)),
            RefPointers(entity2.id, Set(target2.id)),
            RefPointers(target3.id, Set(target1.id))
        )
      )
    )

    val expected = Set(sourceEntity1.toReference, sourceEntity2.toReference)
    runAndWait(
      q.getReferencesTo(wsid, Seq(targetEntity1.toReference, targetEntity2.toReference, targetEntity3.toReference))
    ) should contain theSameElementsAs expected

  }

  behavior of "getReferencesToType"

  it should "find entities" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType1 = "targetType1"
    val targetType2 = "targetType2"
    val targetEntity1 = Entity("target1", targetType1, Map())
    val targetEntity2 = Entity("target2", targetType1, Map())
    val targetEntity3 = Entity("target3", targetType2, Map())
    val target1 = insertAndGet(targetEntity1)
    val target2 = insertAndGet(targetEntity2)
    val target3 = insertAndGet(targetEntity3)

    // create referencing/source entities
    val sourceEntity1 = Entity(
      "entity1",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType2, "target3")
      )
    )
    val sourceEntity2 =
      Entity("entity2",
             "entityType1",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target2"))
      )
    val sourceEntity3 =
      Entity("entity3",
             "entityType2",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"))
      )
    val entity1 = insertAndGet(sourceEntity1)
    val entity2 = insertAndGet(sourceEntity2)
    val entity3 = insertAndGet(sourceEntity3)

    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(entity1.id, Set(target1.id, target3.id)),
            RefPointers(entity2.id, Set(target2.id)),
            RefPointers(entity3.id, Set(target1.id))
        )
      )
    )

    val expected = Set(sourceEntity1.toReference, sourceEntity2.toReference, sourceEntity3.toReference)
    runAndWait(
      q.getReferencesToType(wsid, targetType1)
    ) should contain theSameElementsAs expected

  }

  it should "not return source entities of the same type" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType1 = "targetType1"
    val targetType2 = "targetType2"
    val targetEntity1 = Entity("target1", targetType1, Map())
    val targetEntity2 = Entity("target2", targetType1, Map())
    val targetEntity3 = Entity("target3", targetType2, Map())
    val target1 = insertAndGet(targetEntity1)
    val target2 = insertAndGet(targetEntity2)
    val target3 = insertAndGet(targetEntity3)

    // create referencing/source entities
    val sourceEntity1 = Entity(
      "entity1",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType2, "target3")
      )
    )
    val sourceEntity2 =
      Entity("entity2",
             "entityType1",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target2"))
      )
    val sourceEntity3 =
      Entity("entity3",
             "entityType2",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType1, "target1"))
      )
    val entity1 = insertAndGet(sourceEntity1)
    val entity2 = insertAndGet(sourceEntity2)
    val entity3 = insertAndGet(sourceEntity3)

    // insert rows
    runAndWait(
      q.upsertReferences(
        Set(RefPointers(entity1.id, Set(target1.id, target3.id)),
            RefPointers(entity2.id, Set(target2.id)),
            RefPointers(entity3.id, Set(target1.id))
        )
      )
    )

    val expected = Set(sourceEntity1.toReference, sourceEntity2.toReference, sourceEntity3.toReference)
    runAndWait(
      q.getReferencesToType(wsid, targetType1)
    ) should contain theSameElementsAs expected

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
        ),
        Seq("foo", "bAr")
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
        ),
        Seq("foo", "bAr")
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
          ),
          Seq("foo", "bAr")
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
        entityQuery,
        entityQuery.filterTerms.map(_.split(" ").toSeq).getOrElse(Seq.empty)
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
        ),
        Seq("foo", "bAr")
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
        ),
        Seq("foo", "bAr")
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
    runAndWait(q.batchCreateEntities(workspaceId, entities, allowUpsert = false)) shouldBe entities.size
    // retrieve the entities; retrieved value includes its id
    val recs = entities.map { entity =>
      val actual = runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name))
      actual should not be empty
      val rec = actual.get
      rec.toEntity shouldBe entity
      rec.deleted shouldBe false
      rec.recordVersion shouldBe 0
      rec
    }

    // List all entities of the first entity type and check they match
    if (entities.nonEmpty) {
      val entityType = entities.head.entityType
      val listed = runAndWait(q.listEntities(workspaceId, entityType))
      listed.map(_.toEntity) should contain theSameElementsAs entities.filter(_.entityType == entityType)
    }

    recs
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
