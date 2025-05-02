package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  Entity
}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID

class CompactEntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val q = compactEntityQuery

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
    runAndWait(q.upsertReferences(fromId, toIds)) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIds
    // delete rows
    runAndWait(q.deleteReferences(fromId, Set())) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
  }

  it should "insert and delete partial" in withMinimalTestDatabase { _ =>
    val fromId: Long = 1 // id of the entity doing the referencing: the "source"
    val toIds: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
    // insert rows
    runAndWait(q.upsertReferences(fromId, toIds)) shouldBe toIds.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIds
    // delete rows, keeping the first two from toIds
    val toKeep = toIds.take(2)
    runAndWait(q.deleteReferences(fromId, toKeep)) shouldBe toIds.size - toKeep.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toKeep
  }

  it should "insert and delete non-overlapping" in withMinimalTestDatabase { _ =>
    val fromId: Long = 1 // id of the entity doing the referencing: the "source"
    val toIdsOne: Set[Long] = Set(101, 102, 103, 104, 105) // ids of entities being referenced: the "targets"
    val toIdsTwo: Set[Long] = Set(201, 202, 203) // ids of entities being referenced: the "targets"
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
    // insert rows for set one
    runAndWait(q.upsertReferences(fromId, toIdsOne)) shouldBe toIdsOne.size
    runAndWait(q.getReferencedIds(fromId)) should contain theSameElementsAs toIdsOne
    // delete rows, specifying to keep those in set two (which has no overlap with set one)
    runAndWait(q.deleteReferences(fromId, toIdsTwo)) shouldBe toIdsOne.size
    runAndWait(q.getReferencedIds(fromId)) shouldBe empty
  }

  behavior of "deleteAllReferences"

  it should "delete references for multiple entities" in withMinimalTestDatabase { _ =>
    // ids referencing/source entities
    val fromId1: Long = 1
    val fromId2: Long = 2
    val fromId3: Long = 3
    // ids of referenced/target entities
    val toIds1: Set[Long] = Set(101, 102)
    val toIds2: Set[Long] = Set(103, 104, 105)
    val toIds3: Set[Long] = Set(101, 103, 106)
    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencedIds(fromId1)) shouldBe empty
    runAndWait(q.getReferencedIds(fromId2)) shouldBe empty
    runAndWait(q.getReferencedIds(fromId3)) shouldBe empty
    // insert rows
    runAndWait(q.upsertReferences(fromId1, toIds1)) shouldBe toIds1.size
    runAndWait(q.getReferencedIds(fromId1)) should contain theSameElementsAs toIds1
    runAndWait(q.upsertReferences(fromId2, toIds2)) shouldBe toIds2.size
    runAndWait(q.getReferencedIds(fromId2)) should contain theSameElementsAs toIds2
    runAndWait(q.upsertReferences(fromId3, toIds3)) shouldBe toIds3.size
    runAndWait(q.getReferencedIds(fromId3)) should contain theSameElementsAs toIds3
    // delete rows
    runAndWait(
      q.deleteAllReferences(Set(fromId1, fromId2, fromId3))
    ) shouldBe (toIds1.size + toIds2.size + toIds3.size)
    runAndWait(q.getReferencedIds(fromId1)) shouldBe empty
    runAndWait(q.getReferencedIds(fromId2)) shouldBe empty
    runAndWait(q.getReferencedIds(fromId3)) shouldBe empty
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
    // create the entity to "delete"
    val entity = Entity("entityName",
                        "entityType",
                        Map(
                          AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                          AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                        )
    )
    insertAndGet(entity)

    runAndWait(q.batchHide(wsid, Seq(entity.toReference)))
    val actual = runAndWait(q.getEntity(wsid, entity.entityType, entity.name))
    actual shouldBe empty

    val hidden = runAndWait(q.getDeletedEntity(wsid, entity.entityType, entity.name))
    hidden should not be empty
    hidden.get.attributes shouldBe empty

  }

  behavior of "getReferencingEntities"

  it should "find entities" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType = "targetType"
    val targetEntity1 = Entity("target1", targetType, Map())
    val targetEntity2 = Entity("target2", targetType, Map())
    val targetEntity3 = Entity("target3", targetType, Map())
    val target1 = insertAndGet(targetEntity1)
    val target2 = insertAndGet(targetEntity2)
    val target3 = insertAndGet(targetEntity3)

    // create referencing/source entities
    val sourceEntity1 = Entity(
      "entity1",
      "entityType",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "target1"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "target3")
      )
    )
    val sourceEntity2 =
      Entity("entity2",
             "entityType",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "target2"))
      )
    val sourceEntity3 =
      Entity("entity3",
             "entityType",
             Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference(targetType, "target1"))
      )
    val entity1 = insertAndGet(sourceEntity1)
    val entity2 = insertAndGet(sourceEntity2)
    val entity3 = insertAndGet(sourceEntity3)

    // insert rows
    runAndWait(q.upsertReferences(entity1.id, Set(target1.id, target3.id)))
    runAndWait(q.upsertReferences(entity2.id, Set(target2.id)))
    runAndWait(q.upsertReferences(entity3.id, Set(target1.id)))

    val expected = Set(sourceEntity1.toReference, sourceEntity2.toReference, sourceEntity3.toReference)
    runAndWait(
      q.getReferencingEntities(Set(target1.id, target2.id))
    ) should contain theSameElementsAs expected

  }

  // ====================================================================================================
  //  helpers for tests
  // ====================================================================================================

  private def insertAndGet(entity: Entity, workspaceId: UUID = wsid): CompactEntityRecord = {
    // row should not exist before inserting
    runAndWait(q.getEntity(workspaceId, entity.entityType, entity.name)) shouldBe empty
    // insert the entity
    runAndWait(q.createEntity(workspaceId, entity)) shouldBe 1
    // retrieve the entity; retrieved value includes its id
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
