package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeRename,
  AttributeString,
  AttributeValueList,
  Entity,
  EntityColumnFilter,
  EntityPointer,
  EntityQuery,
  FilterOperators,
  SortDirections,
  WorkspaceFieldSpecs
}
import org.scalatest.Inspectors.forEvery
import slick.dbio.Effect.Read
import slick.jdbc.GetResult
import slick.sql.SqlStreamingAction
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.SQLIntegrityConstraintViolationException
import java.util.UUID
import scala.util.Random

class CompactEntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val ws2id = minimalTestData.workspace2.workspaceIdAsUUID
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

  it should "make no db changes if any of the entities exist" in withMinimalTestDatabase { _ =>
    val entity1 = Entity("entityName1", "entityType", Map())
    val entity2 = Entity("entityName2", "entityType", Map())
    val entity3 = Entity("entityName3", "entityType", Map())

    insertAndGet(entity2)

    val entities = Seq(entity1, entity2, entity3)

    // should throw a primary key violation error
    intercept[SQLIntegrityConstraintViolationException](
      runAndWait(q.batchCreateEntities(wsid, entities, insertOnly = true))
    )

    // entity 2 should still exist
    val rec2 = runAndWait(q.getEntity(wsid, entity2.entityType, entity2.name))
    rec2 shouldBe defined
    rec2.get.toEntity shouldBe entity2

    // entities 1 and 3 should not exist
    runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name)) shouldBe empty
    runAndWait(q.getEntity(wsid, entity3.entityType, entity3.name)) shouldBe empty
  }

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

    val actual = runAndWait(q.getEntityRefs(wsid, Set(entity1.toPointer, entity2.toPointer)))

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
                      Set(entity1.toPointer, EntityPointer(entity2.entityType, "nonexistent-2"), entity2.toPointer)
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

  behavior of "existsAll and countExisting"

  it should "find the entities" in withMinimalTestDatabase { _ =>
    // insert some entities
    val targetType = "target"
    val entity1 = Entity("target1", targetType, Map())
    val entity2 = Entity("target2", targetType, Map())
    val entity3 = Entity("target3", targetType, Map())

    insertAndGet(entity1)
    insertAndGet(entity2)
    insertAndGet(entity3)

    // ask for those entities
    val refs: Set[EntityPointer] = Set(entity1.toPointer, entity2.toPointer, entity3.toPointer)
    runAndWait(q.countExisting(wsid, refs)) shouldBe 3
    runAndWait(q.existsAll(wsid, refs)) shouldBe true

    // ask for a subset of those entities
    val subset = refs.tail
    runAndWait(q.countExisting(wsid, subset)) shouldBe 2
    runAndWait(q.existsAll(wsid, subset)) shouldBe true
  }

  it should "return false/the actual count when not all are found" in withMinimalTestDatabase { _ =>
    // insert some entities
    val targetType = "target"
    val entity1 = Entity("target1", targetType, Map())
    val entity2 = Entity("target2", targetType, Map())
    val entity3 = Entity("target3", targetType, Map())

    insertAndGet(entity1)
    // note we do NOT insert entity2!
    insertAndGet(entity3)

    // ask for those entities
    val refs: Set[EntityPointer] = Set(entity1.toPointer, entity2.toPointer, entity3.toPointer)
    runAndWait(q.countExisting(wsid, refs)) shouldBe 2
    runAndWait(q.existsAll(wsid, refs)) shouldBe false
  }

  behavior of "insertReferences, getReferencesFrom, deleteAllReferencesFrom"

  it should "insert and delete all" in withMinimalTestDatabase { _ =>
    // the entity doing the referencing: the "source"
    val from = EntityPointer("fromType", "fromName")
    // entities being referenced: the "targets"
    val tos: Seq[EntityPointer] = Range(1, 5) map (idx => EntityPointer("toType", s"toName$idx"))

    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencesFrom(wsid, from)) shouldBe empty
    // insert rows
    runAndWait(q.insertReferences(wsid, Set(RefMapping(from, tos.toSet)))) shouldBe tos.size
    runAndWait(q.getReferencesFrom(wsid, from)) should contain theSameElementsAs tos
    // delete rows
    runAndWait(q.deleteAllReferencesFrom(wsid, Set(from))) shouldBe tos.size
    runAndWait(q.getReferencesFrom(wsid, from)) shouldBe empty
  }

  it should "insert for multiple source entities" in withMinimalTestDatabase { _ =>
    // the entities doing the referencing: the "sources"
    val from1 = EntityPointer("fromType", "fromName1")
    val from2 = EntityPointer("fromType", "fromName2")

    // entities being referenced: the "targets"
    val tos1: Seq[EntityPointer] =
      Range(101, 105) map (idx => EntityPointer("toType", s"toName$idx"))
    val tos2: Seq[EntityPointer] =
      (Range(201, 203) map (idx => EntityPointer("toType", s"toName$idx"))) ++ tos1 // notice the overlap

    // sources should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencesFrom(wsid, from1)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, from2)) shouldBe empty
    // insert
    runAndWait(
      q.insertReferences(wsid, Set(RefMapping(from1, tos1.toSet), RefMapping(from2, tos2.toSet)))
    ) shouldBe tos1.size + tos2.size
    runAndWait(q.getReferencesFrom(wsid, from1)) should contain theSameElementsAs tos1
    runAndWait(q.getReferencesFrom(wsid, from2)) should contain theSameElementsAs tos2
  }

  it should "delete references for multiple entities" in withMinimalTestDatabase { _ =>
    // the entities doing the referencing: the "sources"
    val from1 = EntityPointer("fromType", "fromName1")
    val from2 = EntityPointer("fromType", "fromName2")
    val from3 = EntityPointer("fromType", "fromName3")
    // referenced/target entities
    val target1 = EntityPointer("targetType", "targetName1")
    val target2 = EntityPointer("targetType", "targetName2")
    val target3 = EntityPointer("targetType", "targetName3")
    val target4 = EntityPointer("targetType", "targetName4")
    val target5 = EntityPointer("targetType", "targetName5")
    val target6 = EntityPointer("targetType", "targetName6")

    // source should have no rows in ENTITY_REFS table
    runAndWait(q.getReferencesFrom(wsid, from1)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, from2)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, from3)) shouldBe empty

    // references to insert
    val pointers1 = RefMapping(from1, Set(target1, target2))
    val pointers2 = RefMapping(from2, Set(target2, target3, target4))
    val pointers3 = RefMapping(from3, Set(target4, target5, target6))

    // insert rows
    runAndWait(
      q.insertReferences(wsid, Set(pointers1, pointers2, pointers3))
    ) shouldBe (pointers1.to.size + pointers2.to.size + pointers3.to.size)
    runAndWait(q.getReferencesFrom(wsid, from1)) should contain theSameElementsAs pointers1.to
    runAndWait(q.getReferencesFrom(wsid, from2)) should contain theSameElementsAs pointers2.to
    runAndWait(q.getReferencesFrom(wsid, from3)) should contain theSameElementsAs pointers3.to
    // delete rows
    runAndWait(
      q.deleteAllReferencesFrom(wsid, Set(from1, from2, from3))
    ) shouldBe (pointers1.to.size + pointers2.to.size + pointers3.to.size)
    runAndWait(q.getReferencesFrom(wsid, from1)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, from2)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, from3)) shouldBe empty
  }

  it should "only delete references in the given workspace" in withMinimalTestDatabase { _ =>
    // the entities doing the referencing: the "sources"
    val from1 = EntityPointer("fromType", "fromName1")
    // referenced/target entities
    val target1 = EntityPointer("targetType", "targetName1")
    val target2 = EntityPointer("targetType", "targetName2")

    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    // Insert references into different workspaces. Note that both workspaces reuse the "from" entity type/name
    runAndWait(
      q.insertReferences(wsid, Set(RefMapping(from1, Set(target1))))
    )
    runAndWait(
      q.insertReferences(wsid2, Set(RefMapping(from1, Set(target2))))
    )
    runAndWait(q.getReferencesFrom(wsid, from1)) should contain theSameElementsAs Set(target1)
    runAndWait(q.getReferencesFrom(wsid2, from1)) should contain theSameElementsAs Set(target2)
    // delete rows from workspace 1 only
    runAndWait(q.deleteAllReferencesFrom(wsid, Set(from1))) shouldBe 1
    runAndWait(q.getReferencesFrom(wsid, from1)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid2, from1)) should contain theSameElementsAs Set(target2)
  }

  behavior of "deleteAllReferencesFromType"

  it should "only delete references for the given type" in withMinimalTestDatabase { _ =>
    // referencing/source entities
    val sourceType1 = "source1"
    val sourceType2 = "source2"
    val source1 = EntityPointer(sourceType1, "source1") // source1 and source2 have the same type
    val source2 = EntityPointer(sourceType1, "source2")
    val source3 = EntityPointer(sourceType2, "source3") // source3 has a different type

    // referenced/target entities
    val target1 = EntityPointer("targetType", "targetName1")
    val target2 = EntityPointer("targetType", "targetName2")
    val target3 = EntityPointer("targetType", "targetName3")
    val target4 = EntityPointer("targetType", "targetName4")
    val target5 = EntityPointer("targetType", "targetName5")
    val target6 = EntityPointer("targetType", "targetName6")
    // source should have no rows in ENTITY_REFS table
    val pointers1 = RefMapping(source1, Set(target1, target2))
    val pointers2 = RefMapping(source2, Set(target3, target4, target5))
    val pointers3 = RefMapping(source3, Set(target1, target3, target6))
    // insert rows
    runAndWait(
      q.insertReferences(wsid, Set(pointers1, pointers2, pointers3))
    ) shouldBe (pointers1.to.size + pointers2.to.size + pointers3.to.size)
    runAndWait(q.getReferencesFrom(wsid, source1)) should contain theSameElementsAs pointers1.to
    runAndWait(q.getReferencesFrom(wsid, source2)) should contain theSameElementsAs pointers2.to
    runAndWait(q.getReferencesFrom(wsid, source3)) should contain theSameElementsAs pointers3.to
    // delete rows
    runAndWait(
      q.deleteAllReferencesFromType(
        wsid,
        sourceType1
      )
    ) shouldBe (pointers1.to.size + pointers2.to.size)
    runAndWait(q.getReferencesFrom(wsid, source1)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, source2)) shouldBe empty
    runAndWait(q.getReferencesFrom(wsid, source3)) should contain theSameElementsAs pointers3.to
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

  behavior of "attributeExists"

  it should "find attributes" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")

    val entity1 = Entity("entityName1",
                         "entityType",
                         Map(
                           attr1 -> AttributeNumber(1)
                         )
    )
    val entity2 = Entity("entityName2",
                         "entityType",
                         Map(
                           attr2 -> AttributeNumber(1)
                         )
    )
    val entity3 = Entity("entityName3",
                         "entityType",
                         Map(
                           attr3 -> AttributeNumber(1)
                         )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    Seq(attr1, attr2, attr3) foreach { attributeName =>
      withClue(s"attribute $attributeName should exist") {
        val actual = runAndWait(q.attributeExists(wsid, "entityType", attributeName))
        actual shouldBe true
      }
    }

    // some attributes that don't exist
    Seq(AttributeName.fromDelimitedName("import:foo"),
        AttributeName.withDefaultNS("bar"),
        AttributeName.withDefaultNS("boo")
    ) foreach { attributeName =>
      withClue(s"attribute $attributeName should not exist") {
        val actual = runAndWait(q.attributeExists(wsid, "entityType", attributeName))
        actual shouldBe false
      }
    }
  }

  it should "respect the workspace and entity type" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("one")
    val attr2 = AttributeName.withDefaultNS("two")
    val attr3 = AttributeName.withDefaultNS("three")
    val attr4 = AttributeName.withDefaultNS("four")
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    insertAndGet(Entity("entityName", "entityType1", Map(attr1 -> AttributeNumber(1))), wsid)
    insertAndGet(Entity("entityName", "entityType2", Map(attr2 -> AttributeNumber(2))), wsid)
    insertAndGet(Entity("entityName", "entityType1", Map(attr3 -> AttributeNumber(3))), wsid2)
    insertAndGet(Entity("entityName", "entityType2", Map(attr4 -> AttributeNumber(4))), wsid2)

    // helper function to check if the attribute exists in the given workspace and entity type
    def check(attributeName: AttributeName, expectedWorkspaceId: UUID, expectedEntityType: String): Unit =
      Seq(wsid, wsid2) foreach { workspaceId =>
        Seq("entityType1", "entityType2") foreach { entityType =>
          withClue(
            s"attribute $attributeName should only exist in workspace $expectedWorkspaceId and entity type $expectedEntityType;" +
              s" error while checking $workspaceId and $entityType"
          ) {
            val actual = runAndWait(q.attributeExists(workspaceId, entityType, attributeName))
            val expected = workspaceId == expectedWorkspaceId && entityType == expectedEntityType
            actual shouldBe expected
          }
        }
      }

    check(attr1, wsid, "entityType1")
    check(attr2, wsid, "entityType2")
    check(attr3, wsid2, "entityType1")
    check(attr4, wsid2, "entityType2")
  }

  behavior of "anyAttributeExists"

  it should "find attributes" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")

    val entity1 = Entity("entityName1",
                         "entityType",
                         Map(
                           attr1 -> AttributeNumber(1)
                         )
    )
    val entity2 = Entity("entityName2",
                         "entityType",
                         Map(
                           attr2 -> AttributeNumber(1)
                         )
    )
    val entity3 = Entity("entityName3",
                         "entityType",
                         Map(
                           attr3 -> AttributeNumber(1)
                         )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    Seq(attr1, attr2, attr3) foreach { attributeName =>
      withClue(s"attribute $attributeName should exist") {
        val attrsToFind =
          Set(AttributeName.withDefaultNS("nonexistent"),
              attributeName,
              AttributeName.withDefaultNS("anotherNonexistent")
          )
        val actual = runAndWait(q.anyAttributeExists(wsid, "entityType", attrsToFind))
        actual shouldBe true
      }
    }

    // some attributes that don't exist
    Seq(AttributeName.fromDelimitedName("import:foo"),
        AttributeName.withDefaultNS("bar"),
        AttributeName.withDefaultNS("boo")
    ) foreach { attributeName =>
      withClue(s"attribute $attributeName should not exist") {
        val attrsToFind =
          Set(AttributeName.withDefaultNS("nonexistent"),
              attributeName,
              AttributeName.withDefaultNS("anotherNonexistent")
          )
        val actual = runAndWait(q.anyAttributeExists(wsid, "entityType", attrsToFind))
        actual shouldBe false
      }
    }
  }

  it should "respect the workspace and entity type" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("one")
    val attr2 = AttributeName.withDefaultNS("two")
    val attr3 = AttributeName.withDefaultNS("three")
    val attr4 = AttributeName.withDefaultNS("four")
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    insertAndGet(Entity("entityName", "entityType1", Map(attr1 -> AttributeNumber(1))), wsid)
    insertAndGet(Entity("entityName", "entityType2", Map(attr2 -> AttributeNumber(2))), wsid)
    insertAndGet(Entity("entityName", "entityType1", Map(attr3 -> AttributeNumber(3))), wsid2)
    insertAndGet(Entity("entityName", "entityType2", Map(attr4 -> AttributeNumber(4))), wsid2)

    // helper function to check if the attribute exists in the given workspace and entity type
    def check(attributeName: AttributeName, expectedWorkspaceId: UUID, expectedEntityType: String): Unit =
      Seq(wsid, wsid2) foreach { workspaceId =>
        Seq("entityType1", "entityType2") foreach { entityType =>
          withClue(
            s"attribute $attributeName should only exist in workspace $expectedWorkspaceId and entity type $expectedEntityType;" +
              s" error while checking $workspaceId and $entityType"
          ) {
            val attrsToFind =
              Set(AttributeName.withDefaultNS("nonexistent"),
                  attributeName,
                  AttributeName.withDefaultNS("anotherNonexistent")
              )
            val actual = runAndWait(q.anyAttributeExists(workspaceId, entityType, attrsToFind))
            val expected = workspaceId == expectedWorkspaceId && entityType == expectedEntityType
            actual shouldBe expected
          }
        }
      }

    check(attr1, wsid, "entityType1")
    check(attr2, wsid, "entityType2")
    check(attr3, wsid2, "entityType1")
    check(attr4, wsid2, "entityType2")
  }

  behavior of "anyAttributeHasReference"

  it should "find references" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("decoy")

    val entity1 = Entity(
      "entityName1",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity2 = Entity(
      "entityName2",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeEntityReference("entityType", "entityName1"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity3 = Entity(
      "entityName3",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("entityType", "entityName1"),
            AttributeEntityReference("entityType", "entityName2")
          )
        ),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    // attr1 does not have a reference
    val actual1 = runAndWait(q.anyAttributeHasReference(wsid, "entityType", Set(attr1, attr4)))
    withClue("attr1 should not find a reference") {
      actual1 shouldBe false
    }

    // attr2 has a reference
    val actual2 = runAndWait(q.anyAttributeHasReference(wsid, "entityType", Set(attr2, attr4)))
    withClue("attr2 should find a reference scalar") {
      actual2 shouldBe true
    }

    // attr3 has a reference list
    val actual3 = runAndWait(q.anyAttributeHasReference(wsid, "entityType", Set(attr3, attr4)))
    withClue("attr3 should find a reference list") {
      actual3 shouldBe true
    }

  }

  it should "respect the workspace and entity type" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("decoy")

    val attrsMap1 = Map(
      attr1 -> AttributeNumber(1),
      attr2 -> AttributeString("not a reference"),
      attr3 -> AttributeString("not a reference"),
      attr4 -> AttributeString("entityType and entityName are important words")
    )

    val attrsMap2 = Map(
      attr1 -> AttributeNumber(1),
      attr2 -> AttributeEntityReference("entityType1", "entityName1"),
      attr3 -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("entityType1", "entityName1")
        )
      ),
      attr4 -> AttributeString("entityType and entityName are important words")
    )

    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    // insert target entities
    insertAndGet(Entity("entityName1", "entityType1", attrsMap1), wsid)
    insertAndGet(Entity("entityName1", "entityType1", attrsMap1), wsid2)
    // insert the entities we will be searching against.
    // references will exist in wsid/entityType1 and wsid2/entityType2
    insertAndGet(Entity("entityName2", "entityType1", attrsMap2), wsid) // attrsMap2 has references
    insertAndGet(Entity("entityName2", "entityType2", attrsMap1), wsid)
    insertAndGet(Entity("entityName2", "entityType1", attrsMap1), wsid2)
    insertAndGet(Entity("entityName2", "entityType2", attrsMap2), wsid2) // attrsMap2 has references

    // helper function to check if the attribute exists in the given workspace and entity type
    def check(attributeName: AttributeName, workspaceId: UUID, entityType: String, expectedResult: Boolean): Unit = {
      val attrsToFind =
        Set(AttributeName.withDefaultNS("nonexistent"),
            attributeName,
            AttributeName.withDefaultNS("anotherNonexistent")
        )
      val actual = runAndWait(q.anyAttributeHasReference(workspaceId, entityType, attrsToFind))
      actual shouldBe expectedResult
    }

    // in wsid, references exist in entityType1
    check(attr2, wsid, "entityType1", expectedResult = true)
    check(attr3, wsid, "entityType1", expectedResult = true)
    check(attr2, wsid, "entityType2", expectedResult = false)
    check(attr3, wsid, "entityType2", expectedResult = false)
    // in wsid2, references exist in entityType2
    check(attr2, wsid2, "entityType1", expectedResult = false)
    check(attr3, wsid2, "entityType1", expectedResult = false)
    check(attr2, wsid2, "entityType2", expectedResult = true)
    check(attr3, wsid2, "entityType2", expectedResult = true)
  }

  behavior of "deleteEntityAttributes"

  it should "delete the requested attributes" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("decoy")

    val entity1 = Entity(
      "entityName1",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity2 = Entity(
      "entityName2",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeEntityReference("entityType", "entityName1"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity3 = Entity(
      "entityName3",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("entityType", "entityName1"),
            AttributeEntityReference("entityType", "entityName2")
          )
        ),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    // delete attr2 and attr3
    val actual = runAndWait(q.deleteAttributes(wsid, "entityType", Set(attr2, attr3)))
    actual shouldBe 3

    // verify each entity
    forEvery(Seq(entity1, entity2, entity3)) { entity =>
      val ent = runAndWait(q.getEntity(wsid, entity.entityType, entity.name))
      ent shouldBe defined
      ent.get.toEntity.attributes shouldBe Map(attr1 -> AttributeNumber(1),
                                               attr4 -> AttributeString("entityType and entityName are important words")
      )
    }
  }

  it should "respect the workspace and entity type" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("decoy")

    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    val attrsMap = Map(
      attr1 -> AttributeNumber(1),
      attr2 -> AttributeString("not a reference"),
      attr3 -> AttributeString("not a reference"),
      attr4 -> AttributeString("entityType and entityName are important words")
    )

    insertAndGet(Entity("entityName1", "entityType1", attrsMap), wsid)
    insertAndGet(Entity("entityName1", "entityType2", attrsMap), wsid)
    insertAndGet(Entity("entityName1", "entityType1", attrsMap), wsid2)
    insertAndGet(Entity("entityName1", "entityType2", attrsMap), wsid2)

    // delete only workspace1, entityType1
    runAndWait(q.deleteAttributes(wsid, "entityType1", Set(attr2, attr3)))

    // verify each entity
    runAndWait(
      q.getEntity(wsid, "entityType1", "entityName1")
    ).get.toEntity.attributes.keys should contain theSameElementsAs Set(
      attr1,
      attr4
    )
    runAndWait(
      q.getEntity(wsid, "entityType2", "entityName1")
    ).get.toEntity.attributes.keys should contain theSameElementsAs Set(
      attr1,
      attr2,
      attr3,
      attr4
    )
    runAndWait(
      q.getEntity(wsid2, "entityType1", "entityName1")
    ).get.toEntity.attributes.keys should contain theSameElementsAs Set(
      attr1,
      attr2,
      attr3,
      attr4
    )
    runAndWait(
      q.getEntity(wsid2, "entityType2", "entityName1")
    ).get.toEntity.attributes.keys should contain theSameElementsAs Set(
      attr1,
      attr2,
      attr3,
      attr4
    )
  }

  behavior of "deleteAllReferencesFromAttributes"

  it should "delete only the selected references" in withMinimalTestDatabase { _ =>
    import driver.api._

    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("decoy")

    val entity1 = Entity(
      "entityName1",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity2 = Entity(
      "entityName2",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeEntityReference("entityType", "entityName1"),
        attr3 -> AttributeString("not a reference"),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )
    val entity3 = Entity(
      "entityName3",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("entityType", "entityName2")
          )
        ),
        attr4 -> AttributeString("entityType and entityName are important words")
      )
    )

    // create the entities
    insertAndGetAll(Seq(entity1, entity2, entity3))

    val validRef1 =
      // valid ref from entity2 to entity1
      RefMapping(EntityPointer("entityType", "entityName2"), Set(EntityPointer("entityType", "entityName1")))
    val validRef2 =
      // valid ref from entity3 to entity2
      RefMapping(EntityPointer("entityType", "entityName3"), Set(EntityPointer("entityType", "entityName2")))

    val fakeRefs = Set(
      RefMapping(EntityPointer("entityType", "entityName1"), Set(EntityPointer("otherType", "otherName"))),
      RefMapping(EntityPointer("entityType", "entityName2"), Set(EntityPointer("otherType", "otherName"))),
      RefMapping(EntityPointer("entityType", "entityName3"), Set(EntityPointer("otherType", "otherName"))),
      RefMapping(EntityPointer("otherType", "otherName"), Set(EntityPointer("yetAnotherType", "yetAnotherName")))
    )

    // insert the references, including some fake refs
    runAndWait(q.insertReferences(wsid, Set(validRef1, validRef2) ++ fakeRefs))

    // also insert the valid references into a second workspace
    runAndWait(q.insertReferences(testData.workspaceNoAttrs.workspaceIdAsUUID, Set(validRef1, validRef2)))

    // ask to delete references from attr2 and attr3
    val actualDelete = runAndWait(
      q.deleteAllReferencesFromAttributes(wsid, "entityType", Set(attr2, attr3))
    )
    actualDelete shouldBe 2 // should delete the 2 valid references

    val expectedRemaining = fakeRefs.map { fake =>
      RefPointerRecord(wsid,
                       fake.from.entityType,
                       fake.from.entityName,
                       fake.to.head.entityType,
                       fake.to.head.entityName
      )
    } ++ Set(validRef1, validRef2).map { otherWorkspaceRef =>
      RefPointerRecord(
        testData.workspaceNoAttrs.workspaceIdAsUUID,
        otherWorkspaceRef.from.entityType,
        otherWorkspaceRef.from.entityName,
        otherWorkspaceRef.to.head.entityType,
        otherWorkspaceRef.to.head.entityName
      )
    }

    // high-level Slick query to retrieve the remaining references
    val remainingRefs: Seq[RefPointerRecord] = runAndWait(slickDataSource.dataAccess.compactEntityRefSlickQuery.result)

    remainingRefs should contain theSameElementsAs expectedRemaining
  }

  behavior of "renameAttribute"

  it should "change the attribute name" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")

    val renameAttr = AttributeName.withDefaultNS("bar")

    val entity1 = Entity("entityName1",
                         "entityType",
                         Map(
                           attr1 -> AttributeNumber(1),
                           attr2 -> AttributeNumber(2)
                         )
    )
    val entity2 = Entity("entityName2",
                         "entityType",
                         Map(
                           attr2 -> AttributeNumber(2),
                           attr3 -> AttributeNumber(3)
                         )
    )

    insertAndGetAll(Seq(entity1, entity2))

    // rename attr2 ("import:bar") to renameAttr ("bar")
    val rename = runAndWait(q.renameAttribute(wsid, "entityType", attr2, AttributeRename(renameAttr)))
    rename shouldBe 2

    runAndWait(q.getEntity(wsid, "entityType", entity1.name)).get.toEntity.attributes shouldBe Map(
      attr1 -> AttributeNumber(1),
      renameAttr -> AttributeNumber(2)
    )

    runAndWait(q.getEntity(wsid, "entityType", entity2.name)).get.toEntity.attributes shouldBe Map(
      renameAttr -> AttributeNumber(2),
      attr3 -> AttributeNumber(3)
    )
  }

  it should "respect the workspace and entity type" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("one")
    val attr2 = AttributeName.withDefaultNS("two")
    val attr3 = AttributeName.withDefaultNS("three")
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    insertAndGet(Entity("entityName", "entityType1", Map(attr1 -> AttributeNumber(1), attr2 -> AttributeNumber(2))),
                 wsid
    )
    insertAndGet(Entity("entityName", "entityType2", Map(attr2 -> AttributeNumber(2), attr3 -> AttributeNumber(3))),
                 wsid
    )
    insertAndGet(Entity("entityName", "entityType1", Map(attr1 -> AttributeNumber(1), attr2 -> AttributeNumber(2))),
                 wsid2
    )
    insertAndGet(Entity("entityName", "entityType2", Map(attr2 -> AttributeNumber(2), attr3 -> AttributeNumber(3))),
                 wsid2
    )

    // check metadata before any renames
    runAndWait(q.listEntityKeys(wsid)) should contain theSameElementsAs Seq(
      EntityTypeAndAttributeKey("entityType1", attr1),
      EntityTypeAndAttributeKey("entityType1", attr2),
      EntityTypeAndAttributeKey("entityType2", attr2),
      EntityTypeAndAttributeKey("entityType2", attr3)
    )
    runAndWait(q.listEntityKeys(wsid2)) should contain theSameElementsAs Seq(
      EntityTypeAndAttributeKey("entityType1", attr1),
      EntityTypeAndAttributeKey("entityType1", attr2),
      EntityTypeAndAttributeKey("entityType2", attr2),
      EntityTypeAndAttributeKey("entityType2", attr3)
    )

    // rename attr2 in entityType1 and wsid; should only affect entity1 and entity2 in wsid
    val newAttr1 = AttributeName.withDefaultNS("new1")
    runAndWait(q.renameAttribute(wsid, "entityType1", attr2, AttributeRename(newAttr1))) shouldBe 1
    // check metadata
    runAndWait(q.listEntityKeys(wsid)) should contain theSameElementsAs Seq(
      EntityTypeAndAttributeKey("entityType1", attr1),
      EntityTypeAndAttributeKey("entityType1", newAttr1),
      EntityTypeAndAttributeKey("entityType2", attr2),
      EntityTypeAndAttributeKey("entityType2", attr3)
    )
    runAndWait(q.listEntityKeys(wsid2)) should contain theSameElementsAs Seq(
      EntityTypeAndAttributeKey("entityType1", attr1),
      EntityTypeAndAttributeKey("entityType1", attr2),
      EntityTypeAndAttributeKey("entityType2", attr2),
      EntityTypeAndAttributeKey("entityType2", attr3)
    )
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

    runAndWait(q.batchHide(wsid, Seq(entity1.toPointer, entity2.toPointer)))
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

    runAndWait(q.batchHide(wsid, Seq(entity.toPointer)))
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

  behavior of "deleteEntities"

  it should "delete the specified entities in the given workspace" in withMinimalTestDatabase { _ =>
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1) // targeted for deletion
    insertAndGet(entity2) // targeted for deletion
    insertAndGet(entity3) // should be deleted
    insertAndGet(entity4) // targeted for deletion
    insertAndGet(entity4, wsid2) // should NOT be deleted; different workspace

    // validate counts before
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 3),
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )

    // perform the delete
    val pointers = Seq(entity1.toPointer, entity2.toPointer, entity4.toPointer)
    val deletedCount = runAndWait(q.deleteEntities(wsid, pointers))

    deletedCount shouldBe 3

    // validate counts after
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 1),
      (wsid2, entityType1, 1)
    )
  }

  it should "not delete entities which have a foreign key pointed at them" in withMinimalTestDatabase { _ =>
    import driver.api._ // for bespoke SQL queries

    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity5 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity6 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity7 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity8 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1) // targeted for deletion
    insertAndGet(entity2) // targeted for deletion
    insertAndGet(entity3) // should be deleted
    insertAndGet(entity4) // targeted for deletion
    insertAndGet(entity4, wsid2) // should NOT be deleted; different workspace
    val entityRec5 = insertAndGet(entity5) // targeted, but should NOT be deleted due to FK from WORKSPACE_ATTRIBUTE
    val entityRec6 = insertAndGet(entity6) // targeted, but should NOT be deleted due to FK from SUBMISSION
    val entityRec7 = insertAndGet(entity7) // targeted, but should NOT be deleted due to FK from WORKFLOW
    val entityRec8 = insertAndGet(entity8) // targeted, but should NOT be deleted due to FK from SUBMISSION_ATTRIBUTE

    // set up foreign keys

    // a workspace attribute points at entity5
    runAndWait(
      sql"""insert into WORKSPACE_ATTRIBUTE(name, owner_id, value_entity_ref)
            values ('n', $wsid, ${entityRec5.id})""".asUpdate
    )
    // a submission points at entity6
    runAndWait(
      sql"""insert into METHOD_CONFIG(NAMESPACE, NAME, WORKSPACE_ID)
            values ('nn', 'n', $wsid)""".asUpdate
    )
    val methodConfigId = runAndWait(
      sql"select ID from METHOD_CONFIG where NAMESPACE = 'nn' and NAME = 'n' and WORKSPACE_ID = $wsid".as[Long].head
    )
    runAndWait(sql"""insert into SUBMISSION(ID, WORKSPACE_ID, SUBMITTER, METHOD_CONFIG_ID, SUBMISSION_ROOT, ENTITY_ID)
            values (${UUID.randomUUID()}, $wsid, 'submitter', $methodConfigId, 'subroot', ${entityRec6.id})""".asUpdate)
    val submissionId = runAndWait(sql"select ID from SUBMISSION where WORKSPACE_ID = $wsid".as[UUID].head)
    // a workflow points at entity7
    runAndWait(sql"""insert into WORKFLOW(SUBMISSION_ID, record_version, ENTITY_ID)
        values($submissionId, 1, ${entityRec7.id})""".asUpdate)
    val workflowId = runAndWait(sql"select ID from WORKFLOW where SUBMISSION_ID = $submissionId".as[Long].head)
    // a submission attribute points at entity8
    runAndWait(
      sql"""insert into SUBMISSION_VALIDATION(WORKFLOW_ID, INPUT_NAME) values($workflowId, 'input')""".asUpdate
    )
    val submissionValidationId =
      runAndWait(sql"select id from SUBMISSION_VALIDATION where WORKFLOW_ID = $workflowId".as[Long].head)
    runAndWait(
      sql"""insert into SUBMISSION_ATTRIBUTE(name, owner_id, value_entity_ref)
            values ('s', $submissionValidationId, ${entityRec8.id})""".asUpdate
    )

    // validate counts before
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 7),
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )

    // perform the delete
    val pointers = Seq(entity1.toPointer,
                       entity2.toPointer,
                       entity4.toPointer,
                       entity5.toPointer,
                       entity6.toPointer,
                       entity7.toPointer,
                       entity8.toPointer
    )
    val deletedCount = runAndWait(q.deleteEntities(wsid, pointers))

    deletedCount shouldBe 3

    // validate counts after
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 5),
      (wsid2, entityType1, 1)
    )
  }

  behavior of "deleteEntitiesOfType"

  it should "delete entities of the given type in the given workspace" in withMinimalTestDatabase { _ =>
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1) // should be deleted
    insertAndGet(entity2) // should NOT be deleted; different entity type
    insertAndGet(entity3) // should be deleted
    insertAndGet(entity4) // should be deleted
    insertAndGet(entity4, wsid2) // should NOT be deleted; different workspace

    // validate counts before
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 3),
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )

    // perform the delete
    val deletedCount = runAndWait(q.deleteEntitiesOfType(wsid, entityType1))

    deletedCount shouldBe 3

    // validate counts after
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )
  }

  it should "not delete entities which have a foreign key pointed at them" in withMinimalTestDatabase { _ =>
    import driver.api._ // for bespoke SQL queries

    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    val entityType1 = "entityType1"
    val entityType2 = "entityType2"
    val entity1 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity2 = Entity(UUID.randomUUID().toString, entityType2, Map())
    val entity3 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity4 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity5 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity6 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity7 = Entity(UUID.randomUUID().toString, entityType1, Map())
    val entity8 = Entity(UUID.randomUUID().toString, entityType1, Map())
    insertAndGet(entity1) // should be deleted
    insertAndGet(entity2) // should NOT be deleted; different entity type
    insertAndGet(entity3) // should be deleted
    insertAndGet(entity4) // should be deleted
    insertAndGet(entity4, wsid2) // should NOT be deleted; different workspace
    val entityRec5 = insertAndGet(entity5) // should NOT be deleted due to FK from WORKSPACE_ATTRIBUTE
    val entityRec6 = insertAndGet(entity6) // should NOT be deleted due to FK from SUBMISSION
    val entityRec7 = insertAndGet(entity7) // should NOT be deleted due to FK from WORKFLOW
    val entityRec8 = insertAndGet(entity8) // should NOT be deleted due to FK from SUBMISSION_ATTRIBUTE

    // set up foreign keys

    // a workspace attribute points at entity5
    runAndWait(
      sql"""insert into WORKSPACE_ATTRIBUTE(name, owner_id, value_entity_ref)
            values ('n', $wsid, ${entityRec5.id})""".asUpdate
    )
    // a submission points at entity6
    runAndWait(
      sql"""insert into METHOD_CONFIG(NAMESPACE, NAME, WORKSPACE_ID)
            values ('nn', 'n', $wsid)""".asUpdate
    )
    val methodConfigId = runAndWait(
      sql"select ID from METHOD_CONFIG where NAMESPACE = 'nn' and NAME = 'n' and WORKSPACE_ID = $wsid".as[Long].head
    )
    runAndWait(sql"""insert into SUBMISSION(ID, WORKSPACE_ID, SUBMITTER, METHOD_CONFIG_ID, SUBMISSION_ROOT, ENTITY_ID)
            values (${UUID.randomUUID()}, $wsid, 'submitter', $methodConfigId, 'subroot', ${entityRec6.id})""".asUpdate)
    val submissionId = runAndWait(sql"select ID from SUBMISSION where WORKSPACE_ID = $wsid".as[UUID].head)
    // a workflow points at entity7
    runAndWait(sql"""insert into WORKFLOW(SUBMISSION_ID, record_version, ENTITY_ID)
        values($submissionId, 1, ${entityRec7.id})""".asUpdate)
    val workflowId = runAndWait(sql"select ID from WORKFLOW where SUBMISSION_ID = $submissionId".as[Long].head)
    // a submission attribute points at entity8
    runAndWait(
      sql"""insert into SUBMISSION_VALIDATION(WORKFLOW_ID, INPUT_NAME) values($workflowId, 'input')""".asUpdate
    )
    val submissionValidationId =
      runAndWait(sql"select id from SUBMISSION_VALIDATION where WORKFLOW_ID = $workflowId".as[Long].head)
    runAndWait(
      sql"""insert into SUBMISSION_ATTRIBUTE(name, owner_id, value_entity_ref)
            values ('s', $submissionValidationId, ${entityRec8.id})""".asUpdate
    )

    // validate counts before
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 7),
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )

    // perform the delete
    val deletedCount = runAndWait(q.deleteEntitiesOfType(wsid, entityType1))

    deletedCount shouldBe 3

    // validate counts after
    getRawCounts should contain theSameElementsAs Seq(
      (wsid, entityType1, 4),
      (wsid, entityType2, 1),
      (wsid2, entityType1, 1)
    )
  }

  behavior of "getReferencesTo"

  it should "find entities" in withMinimalTestDatabase { _ =>
    // referencing/source entities
    val sourceType1 = "source1"
    val sourceType2 = "source2"
    val source1 = EntityPointer(sourceType1, "source1") // source1 and source2 have the same type
    val source2 = EntityPointer(sourceType1, "source2")
    val source3 = EntityPointer(sourceType2, "source3") // source3 has a different type

    // referenced/target entities
    val target1 = EntityPointer("targetType", "targetName1")
    val target2 = EntityPointer("targetType", "targetName2")
    val target3 = EntityPointer("targetType", "targetName3")
    val target4 = EntityPointer("targetType", "targetName4")
    val target5 = EntityPointer("targetType", "targetName5")
    val target6 = EntityPointer("targetType", "targetName6")
    // source should have no rows in ENTITY_REFS table
    val pointers1 = RefMapping(source1, Set(target1, target2))
    val pointers2 = RefMapping(source2, Set(target3, target4, target5))
    val pointers3 = RefMapping(source3, Set(target1, target3, target6))
    // insert rows
    runAndWait(
      q.insertReferences(wsid, Set(pointers1, pointers2, pointers3))
    ) shouldBe (pointers1.to.size + pointers2.to.size + pointers3.to.size)
    runAndWait(q.getReferencesFrom(wsid, source1)) should contain theSameElementsAs pointers1.to
    runAndWait(q.getReferencesFrom(wsid, source2)) should contain theSameElementsAs pointers2.to
    runAndWait(q.getReferencesFrom(wsid, source3)) should contain theSameElementsAs pointers3.to

    // target1 is referenced by source1 and source3
    runAndWait(q.getReferencesTo(wsid, Seq(target1))) should contain theSameElementsAs Seq(source1, source3)
    // target5 is referenced by source2
    runAndWait(q.getReferencesTo(wsid, Seq(target5))) should contain theSameElementsAs Seq(source2)
    // target6 is referenced by source3
    runAndWait(q.getReferencesTo(wsid, Seq(target6))) should contain theSameElementsAs Seq(source3)

    // target2 is referenced by source1; target6 is referenced by source3
    runAndWait(q.getReferencesTo(wsid, Seq(target2, target6))) should contain theSameElementsAs Seq(source1, source3)

  }

  it should "not find entities in other workspaces" in withMinimalTestDatabase { _ =>
    val wsid2 = minimalTestData.workspace2.workspaceIdAsUUID

    // referencing/source entities
    val source1 = EntityPointer("sourceType", "source1")
    val source2 = EntityPointer("sourceType", "source2")

    // referenced/target entities
    val target1 = EntityPointer("targetType", "targetName1")

    val pointers1 = RefMapping(source1, Set(target1))
    val pointers2 = RefMapping(source2, Set(target1))
    // insert rows for workspace 1
    runAndWait(q.insertReferences(wsid, Set(pointers1))) shouldBe pointers1.to.size
    // insert rows for workspace 2
    runAndWait(q.insertReferences(wsid2, Set(pointers2))) shouldBe pointers2.to.size

    runAndWait(q.getReferencesTo(wsid, Seq(target1))) should contain theSameElementsAs Seq(pointers1.from)
    runAndWait(q.getReferencesTo(wsid2, Seq(target1))) should contain theSameElementsAs Seq(pointers2.from)
  }

  it should "exclude entities included in the search" in withMinimalTestDatabase { _ =>
    // define entities
    val entity1 = EntityPointer("entityType", "name1")
    val entity2 = EntityPointer("entityType", "name2")
    val entity3 = EntityPointer("entityType", "name3")

    // entity2 references entity1; entity3 references both entity1 and entity2
    val pointers1 = RefMapping(entity2, Set(entity1))
    val pointers2 = RefMapping(entity3, Set(entity1, entity2))

    runAndWait(q.insertReferences(wsid, Set(pointers1, pointers2))) shouldBe pointers1.to.size + pointers2.to.size

    // asking for references to entity1 and entity2 should exclude entity2 because entity2 is in the search criteria,
    //  even though entity2 references entity1
    val expected = Set(entity3)
    runAndWait(q.getReferencesTo(wsid, Seq(entity1, entity2))).toSet should contain theSameElementsAs expected

  }

  behavior of "getReferencesToType"

  it should "find entities" in withMinimalTestDatabase { _ =>
    // create referenced/target entities
    val targetType1 = "targetType1"
    val targetType2 = "targetType2"
    val target1 = EntityPointer(targetType1, "target1") // target 1 and 2 use the same type
    val target2 = EntityPointer(targetType1, "target2")
    val target3 = EntityPointer(targetType2, "target3") // target 3 is a different type

    // create referencing/source entities
    val source1 = EntityPointer("entityType1", "entity1") // source 1 and 2 use the same type
    val source2 = EntityPointer("entityType1", "entity2")
    val source3 = EntityPointer("entityType2", "entity3") // source 3 is a different type

    val pointers1 = RefMapping(source1, Set(target3))
    val pointers2 = RefMapping(source2, Set(target2))
    val pointers3 = RefMapping(source3, Set(target1))

    runAndWait(
      q.insertReferences(wsid, Set(pointers1, pointers2, pointers3))
    ) shouldBe pointers1.to.size + pointers2.to.size + pointers3.to.size

    val expected = Set(source2, source3)
    runAndWait(q.getReferencesToType(wsid, targetType1)) should contain theSameElementsAs expected
  }

  it should "not return source entities of the same type" in withMinimalTestDatabase { _ =>
    // define entities
    val entity1 = EntityPointer("entityTypeA", "name1")
    val entity2 = EntityPointer("entityTypeA", "name2")
    val entity3 = EntityPointer("entityTypeB", "name3")

    // entity2 references entity1; entity3 references both entity1 and entity2
    val pointers1 = RefMapping(entity2, Set(entity1))
    val pointers2 = RefMapping(entity3, Set(entity1, entity2))

    // insert rows
    runAndWait(
      q.insertReferences(wsid, Set(pointers1, pointers2))
    ) shouldBe pointers1.to.size + pointers2.to.size

    // asking for references to entity1 should exclude entity2 because entity2 is of the same type,
    //  even though entity2 references entity1
    runAndWait(q.getReferencesToType(wsid, "entityTypeA")).toSet should contain theSameElementsAs Set(entity3)

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

  behavior of "listEntities"

  it should "handle entities with simple attributes" in withMinimalTestDatabase { _ =>
    val testEntityType = "testEntityType"
    val entity1 = Entity("entityName1",
                         testEntityType,
                         Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
    )
    val entity2 = Entity("entityName2",
                         testEntityType,
                         Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
    )
    val ws1Entities = Seq(
      entity1,
      entity2,
      Entity("entityName3",
             "otherEntityType",
             Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
      )
    )

    val ws2Entities = Seq(
      Entity("entityName4",
             testEntityType,
             Map(AttributeName.withDefaultNS("foo") -> AttributeString(UUID.randomUUID().toString))
      )
    )

    // insert the entities
    runAndWait(q.batchCreateEntities(wsid, ws1Entities, insertOnly = true)) shouldBe ws1Entities.size
    runAndWait(q.batchCreateEntities(ws2id, ws2Entities, insertOnly = true)) shouldBe ws2Entities.size

    // validate listed entities of entityType "testEntityType" in the first workspace
    runAndWait(q.listEntities(wsid, testEntityType)).map(_.toEntity) should contain theSameElementsAs Seq(entity1,
                                                                                                          entity2
    )
  }

  behavior of "renameEntity"

  it should "rename an entity and update all references" in withMinimalTestDatabase { _ =>
    // Create the original entity
    val entityType = "testType"
    val originalEntity = Entity("originalName", entityType, Map())
    insertAndGet(originalEntity)

    // Create source entities referencing the original entity
    val sourceEntity1 = Entity(
      "sourceEntity1",
      "sourceType",
      Map(
        AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
          Seq(originalEntity.toReference)
        )
      )
    )
    val sourceEntity2 = Entity(
      "sourceEntity2",
      "sourceType",
      Map(
        AttributeName.withDefaultNS("ref") -> originalEntity.toReference
      )
    )
    insertAndGetAll(Seq(sourceEntity1, sourceEntity2))

    val refMappings = Set(
      RefMapping(sourceEntity1.toPointer, Set(originalEntity.toPointer)),
      RefMapping(sourceEntity2.toPointer, Set(originalEntity.toPointer))
    )
    runAndWait(q.insertReferences(wsid, refMappings))

    // Verify references to the original entity before rename
    runAndWait(q.getReferencesTo(wsid, Seq(originalEntity.toPointer))) should contain theSameElementsAs Seq(
      sourceEntity1.toPointer,
      sourceEntity2.toPointer
    )

    // Rename the entity
    val newName = "newName"
    runAndWait(q.renameEntity(wsid, entityType, originalEntity.name, newName)) shouldBe 1

    // Verify the entity was renamed
    val renamedEntity = runAndWait(q.getEntity(wsid, entityType, newName)).get.toEntity
    renamedEntity.name shouldBe newName

    // Verify the old name no longer exists
    runAndWait(q.getEntity(wsid, entityType, originalEntity.name)) shouldBe None

    // Verify references in source entities are updated
    val updatedSourceEntity1 = runAndWait(q.getEntity(wsid, "sourceType", sourceEntity1.name)).get.toEntity
    updatedSourceEntity1
      .attributes(AttributeName.withDefaultNS("refList"))
      .asInstanceOf[AttributeEntityReferenceList]
      .list should contain theSameElementsAs Seq(renamedEntity.toReference)

    val updatedSourceEntity2 = runAndWait(q.getEntity(wsid, "sourceType", sourceEntity2.name)).get.toEntity
    updatedSourceEntity2.attributes(AttributeName.withDefaultNS("ref")) shouldBe renamedEntity.toReference

    // Verify references to the renamed entity after rename
    runAndWait(q.getReferencesTo(wsid, Seq(renamedEntity.toPointer))) should contain theSameElementsAs Seq(
      sourceEntity1.toPointer,
      sourceEntity2.toPointer
    )
  }

  behavior of "renameEntityType"

  it should "rename an entity type and update all references" in withMinimalTestDatabase { _ =>
    // Create target entities
    val targetType = "targetType"
    val targetEntity1 = Entity("targetEntity1", targetType, Map())
    val targetEntity2 = Entity("targetEntity2", targetType, Map())

    // Create source entities
    val sourceType = "sourceType"
    val sourceEntity1 = Entity(
      "sourceEntity1",
      sourceType,
      Map(
        AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
          Seq(
            targetEntity1.toReference,
            targetEntity2.toReference
          )
        )
      )
    )
    val sourceEntity2 = Entity("sourceEntity2",
                               sourceType,
                               Map(
                                 AttributeName.withDefaultNS("ref") -> targetEntity2.toReference
                               )
    )
    val sourceEntity3 = Entity("sourceEntity3",
                               sourceType,
                               Map(
                                 AttributeName.withDefaultNS("refSingletonList") -> AttributeEntityReferenceList(
                                   Seq(
                                     targetEntity1.toReference
                                   )
                                 )
                               )
    )

    // Insert all entities
    insertAndGetAll(Seq(targetEntity1, targetEntity2, sourceEntity1, sourceEntity2, sourceEntity3))

    val refMappings = Set(
      RefMapping(sourceEntity1.toPointer, Set(targetEntity1.toPointer, targetEntity2.toPointer)),
      RefMapping(sourceEntity2.toPointer, Set(targetEntity2.toPointer)),
      RefMapping(sourceEntity3.toPointer, Set(targetEntity1.toPointer))
    )

    runAndWait(q.insertReferences(wsid, refMappings))

    // Execute renameEntityType and verify the result
    val newTargetType = "newTargetType"
    runAndWait(q.renameEntityType(wsid, targetType, newTargetType)) shouldBe 2

    val newTargetEntity1 = targetEntity1.copy(entityType = newTargetType)
    val newTargetEntity2 = targetEntity2.copy(entityType = newTargetType)

    // Verify entity types were updated in the ENTITY table
    val newTargets = runAndWait(q.listEntities(wsid, newTargetType))
    newTargets.map(_.toEntity) should contain theSameElementsAs Seq(newTargetEntity1, newTargetEntity2)

    // Verify from_entity_type was updated in ENTITY_REFS table
    runAndWait(q.getReferencesFrom(wsid, sourceEntity1.toPointer)) should contain theSameElementsAs Seq(
      newTargetEntity1.toPointer,
      newTargetEntity2.toPointer
    )
    runAndWait(q.getReferencesFrom(wsid, sourceEntity2.toPointer)) should contain theSameElementsAs Seq(
      newTargetEntity2.toPointer
    )
    runAndWait(q.getReferencesFrom(wsid, sourceEntity3.toPointer)) should contain theSameElementsAs Seq(
      newTargetEntity1.toPointer
    )

    // Verify old references no longer exist
    runAndWait(q.getReferencesTo(wsid, Seq(targetEntity1.toPointer))) shouldBe empty
    runAndWait(q.getReferencesTo(wsid, Seq(targetEntity2.toPointer))) shouldBe empty

    // Verify to_entity_type was updated in ENTITY_REFS table
    runAndWait(q.getReferencesTo(wsid, Seq(newTargetEntity1.toPointer))) should contain theSameElementsAs Seq(
      sourceEntity1.toPointer,
      sourceEntity3.toPointer
    )
    runAndWait(q.getReferencesTo(wsid, Seq(newTargetEntity2.toPointer))) should contain theSameElementsAs Seq(
      sourceEntity1.toPointer,
      sourceEntity2.toPointer
    )

    // Verify references in entity attributes are updated
    val updatedSourceEntity1 = runAndWait(q.getEntity(wsid, sourceType, sourceEntity1.name)).get.toEntity
    updatedSourceEntity1
      .attributes(AttributeName.withDefaultNS("refList"))
      .asInstanceOf[AttributeEntityReferenceList]
      .list should contain theSameElementsAs Seq(
      newTargetEntity1.toReference,
      newTargetEntity2.toReference
    )
    val updatedSourceEntity2 = runAndWait(q.getEntity(wsid, sourceType, sourceEntity2.name)).get.toEntity
    updatedSourceEntity2.attributes(AttributeName.withDefaultNS("ref")) shouldBe newTargetEntity2.toReference

    val updatedSourceEntity3 = runAndWait(q.getEntity(wsid, sourceType, sourceEntity3.name)).get.toEntity
    updatedSourceEntity3
      .attributes(AttributeName.withDefaultNS("refSingletonList"))
      .asInstanceOf[AttributeEntityReferenceList]
      .list should contain theSameElementsAs Seq(
      newTargetEntity1.toReference
    )
    // Verify entity keys table is updated
    import driver.api._

    val entityKeysQuery1 =
      sql"""select entity_type from ENTITY_KEYS where workspace_id = $wsid and entity_type = $targetType""".as[String]
    runAndWait(entityKeysQuery1).size shouldBe 0
    val entityKeysQuery2 =
      sql"""select entity_type from ENTITY_KEYS where workspace_id = $wsid and entity_type = $newTargetType"""
        .as[String]
    runAndWait(entityKeysQuery2).size shouldBe 2
  }

  it should "not change non refereces that match the old name" in withMinimalTestDatabase { _ =>
    // Create target entities
    val targetType = "targetType"
    val targetEntity1 = Entity("targetEntity1", targetType, Map())

    // Create source entities
    val sourceType = "sourceType"
    val sourceEntity1 = Entity(
      "sourceEntity1",
      sourceType,
      Map(
        AttributeName.withDefaultNS("trixyList") -> AttributeValueList(
          Seq(
            AttributeString(targetType),
            AttributeString(targetType)
          )
        )
      )
    )
    val sourceEntity2 = Entity("sourceEntity2",
                               sourceType,
                               Map(
                                 AttributeName.withDefaultNS("trixyVal") -> AttributeString(targetType)
                               )
    )
    val sourceEntity3 = Entity("sourceEntity3",
                               sourceType,
                               Map(
                                 AttributeName.withDefaultNS("trixySingletonList") -> AttributeValueList(
                                   Seq(
                                     AttributeString(targetType)
                                   )
                                 )
                               )
    )

    // Insert all entities
    insertAndGetAll(Seq(targetEntity1, sourceEntity1, sourceEntity2, sourceEntity3))

    // these references does not exist in the attributes but makes the underlying rename query think so
    runAndWait(
      q.insertReferences(
        wsid,
        Set(
          RefMapping(sourceEntity1.toPointer, Set(targetEntity1.toPointer)),
          RefMapping(sourceEntity2.toPointer, Set(targetEntity1.toPointer)),
          RefMapping(sourceEntity3.toPointer, Set(targetEntity1.toPointer))
        )
      )
    )

    // Execute renameEntityType and verify the result
    val newTargetType = "newTargetType"
    runAndWait(q.renameEntityType(wsid, targetType, newTargetType)) shouldBe 1

    val newTargetEntity1 = targetEntity1.copy(entityType = newTargetType)

    // Verify entity types were updated in the ENTITY table
    val newTargets = runAndWait(q.listEntities(wsid, newTargetType))
    newTargets.map(_.toEntity) should contain theSameElementsAs Seq(newTargetEntity1)

    // Verify source is unchanged
    val sourceEntities = runAndWait(q.listEntities(wsid, sourceType))
    sourceEntities.map(_.toEntity) should contain theSameElementsAs Seq(
      sourceEntity1,
      sourceEntity2,
      sourceEntity3
    )
  }

  it should "handle no references correctly" in withMinimalTestDatabase { _ =>
    // Create target entities
    val targetType = "targetType"
    val targetEntity1 = Entity("targetEntity1", targetType, Map())

    // Create source entities
    val sourceType = "sourceType"
    val sourceEntity1 = Entity("sourceEntity1",
                               sourceType,
                               Map(
                                 AttributeName.withDefaultNS("ref") -> targetEntity1.toReference
                               )
    )

    // Insert all entities
    insertAndGetAll(Seq(targetEntity1, sourceEntity1))

    runAndWait(
      q.insertReferences(wsid,
                         Set(
                           RefMapping(sourceEntity1.toPointer, Set(targetEntity1.toPointer))
                         )
      )
    )

    // Execute renameEntityType, this time on sourceType which has no references
    val newSourceType = "newSourceType"
    val result = runAndWait(q.renameEntityType(wsid, sourceType, newSourceType))

    // Verify the entity was updated
    result shouldBe 1

    // Get the updated entity
    val updatedSource = runAndWait(q.getEntity(wsid, newSourceType, sourceEntity1.name))

    // Verify the entity exists with the new type
    val newSourceEntity = sourceEntity1.copy(entityType = newSourceType)
    updatedSource.map(_.toEntity) shouldBe Some(newSourceEntity)

    // Verify the references were updated
    runAndWait(q.getReferencesFrom(wsid, newSourceEntity.toPointer)) should contain theSameElementsAs Seq(
      targetEntity1.toPointer
    )
  }

  it should "handle a list with 10000 entity references efficiently" in withMinimalTestDatabase { _ =>
    // Create target entity type that will be renamed
    val targetType = "largeRefTargetType"
    val targetEntity = Entity("targetEntity", targetType, Map())

    // Create a source entity with a list of 1000 references to the target entity
    val sourceType = "largeRefSourceType"
    val refList = AttributeEntityReferenceList(
      (1 to 10000).map(_ => targetEntity.toReference).toSeq
    )
    val sourceEntity = Entity("sourceWithLargeRefList",
                              sourceType,
                              Map(
                                AttributeName.withDefaultNS("refs") -> refList
                              )
    )

    // Insert entities
    insertAndGet(targetEntity)
    insertAndGet(sourceEntity)

    // Insert references - we need 1000 identical references
    val refMapping = RefMapping(sourceEntity.toPointer, Set(targetEntity.toPointer))
    runAndWait(q.insertReferences(wsid, Set(refMapping)))

    // Execute renameEntityType and verify the result
    val newTargetType = "newLargeRefTargetType"
    val updateCount = runAndWait(q.renameEntityType(wsid, targetType, newTargetType))

    // Should have updated 1 entity (the target)
    updateCount shouldBe 1

    // Verify target entity was renamed
    val newTargetEntity = targetEntity.copy(entityType = newTargetType)
    val renamedTargets = runAndWait(q.listEntities(wsid, newTargetType))
    renamedTargets.map(_.toEntity) should contain theSameElementsAs Seq(newTargetEntity)

    // Verify old entity type no longer exists
    runAndWait(q.listEntities(wsid, targetType)) shouldBe empty

    // Verify references were updated in the source entity's attribute
    val updatedSource = runAndWait(q.getEntity(wsid, sourceType, sourceEntity.name)).get.toEntity
    val updatedRefList = updatedSource
      .attributes(AttributeName.withDefaultNS("refs"))
      .asInstanceOf[AttributeEntityReferenceList]

    // Check all 1000 references were updated correctly
    updatedRefList.list.size shouldBe 10000
    updatedRefList.list.foreach { ref =>
      ref.entityType shouldBe newTargetType
      ref.entityName shouldBe targetEntity.name
    }

    // Verify ENTITY_REFS was updated
    val updatedRefs = runAndWait(q.getReferencesFrom(wsid, sourceEntity.toPointer))
    updatedRefs should contain theSameElementsAs Seq(newTargetEntity.toPointer)
  }

  behavior of "copyEntities"

  it should "copy entities from source workspace to destination workspace" in withMinimalTestDatabase { _ =>
    val sourceWorkspaceId = minimalTestData.workspace.workspaceIdAsUUID
    val destinationWorkspaceId = minimalTestData.workspace2.workspaceIdAsUUID

    val entity1 =
      Entity("entityName1", "entityType1", Map(AttributeName.withDefaultNS("attr1") -> AttributeString("value1")))
    val entity2 =
      Entity("entityName2", "entityType1", Map(AttributeName.withDefaultNS("attr2") -> AttributeNumber(42)))
    val entity3 =
      Entity("entityName3", "entityType2", Map(AttributeName.withDefaultNS("attr3") -> AttributeString("value3")))
    insertAndGetAll(Seq(entity1, entity2, entity3), sourceWorkspaceId)

    val copiedEntitiesResult = runAndWait(
      q.copyEntities(sourceWorkspaceId,
                     destinationWorkspaceId,
                     Set(entity1.toPointer, entity2.toPointer, entity3.toPointer)
      )
    )

    copiedEntitiesResult shouldBe 3

    val copiedEntities = runAndWait(q.listEntities(destinationWorkspaceId, "entityType1")) ++ runAndWait(
      q.listEntities(destinationWorkspaceId, "entityType2")
    )
    copiedEntities.map(_.toEntity) should contain theSameElementsAs Seq(entity1, entity2, entity3)

  }

  it should "copy entityRefs from source workspace to destination workspace" in withMinimalTestDatabase { _ =>
    // Define source and destination workspaces
    val sourceWorkspaceId = minimalTestData.workspace.workspaceIdAsUUID
    val destinationWorkspaceId = minimalTestData.workspace2.workspaceIdAsUUID

    // Create entities in the source workspace
    val entity1 =
      Entity("entityName1", "entityType1", Map(AttributeName.withDefaultNS("attr1") -> AttributeString("value1")))
    val entity2 = Entity("entityName2", "entityType1", Map(AttributeName.withDefaultNS("attr2") -> AttributeNumber(42)))
    val entity3 =
      Entity("entityName3", "entityType2", Map(AttributeName.withDefaultNS("attr3") -> AttributeString("value3")))

    // Insert entity references between entities
    val refMapping = RefMapping(entity1.toPointer, Set(entity2.toPointer, entity3.toPointer))
    runAndWait(q.insertReferences(sourceWorkspaceId, Set(refMapping)))

    // Perform the copy operation
    val copiedEntityRefsResult =
      runAndWait(q.copyEntityReferences(sourceWorkspaceId, destinationWorkspaceId, Set(entity1.toPointer)))

    // Verify the result
    copiedEntityRefsResult shouldBe 2

    // Verify references in the destination workspace
    val copiedReferences = runAndWait(q.getReferencesFrom(destinationWorkspaceId, entity1.toPointer))
    copiedReferences should contain theSameElementsAs Seq(entity2.toPointer, entity3.toPointer)
  }

  it should "get recursive entity references" in withMinimalTestDatabase { _ =>
    // Define workspace
    val workspaceId = minimalTestData.workspace.workspaceIdAsUUID

    // Create entities
    val entity1 = EntityPointer("entityType1", "entityName1")
    val entity2 = EntityPointer("entityType1", "entityName2")
    val entity3 = EntityPointer("entityType1", "entityName3")
    val entity4 = EntityPointer("entityType1", "entityName4")

    // Insert references to form a recursive structure
    val refMapping1 = RefMapping(entity1, Set(entity2, entity3))
    val refMapping2 = RefMapping(entity2, Set(entity4))
    val refMapping3 = RefMapping(entity3, Set(entity4))

    runAndWait(q.insertReferences(workspaceId, Set(refMapping1, refMapping2, refMapping3)))

    // Perform the recursive query
    val recursiveReferences = runAndWait(q.recursiveGetEntityReferences(workspaceId, Set(entity1)))

    // Verify the result
    recursiveReferences should contain theSameElementsAs Set(
      RefMapping(entity1, Set(entity2, entity3)),
      RefMapping(entity2, Set(entity4)),
      RefMapping(entity3, Set(entity4))
    )
  }

  it should "get recursive entity references with cycles" in withMinimalTestDatabase { _ =>
    // Define workspace
    val workspaceId = minimalTestData.workspace.workspaceIdAsUUID

    // Create entities
    val entity1 = EntityPointer("entityType1", "entityName1")
    val entity2 = EntityPointer("entityType1", "entityName2")

    // Insert references to form a recursive structure
    val refMapping1 = RefMapping(entity1, Set(entity2))
    val refMapping2 = RefMapping(entity2, Set(entity1))

    runAndWait(q.insertReferences(workspaceId, Set(refMapping1, refMapping2)))

    // Perform the recursive query
    val recursiveReferences = runAndWait(q.recursiveGetEntityReferences(workspaceId, Set(entity1)))

    // Verify the result
    recursiveReferences should contain theSameElementsAs Set(
      RefMapping(entity1, Set(entity2)),
      RefMapping(entity2, Set(entity1))
    )
  }

  // ====================================================================================================
  //  helpers for tests
  // ====================================================================================================

  // This does NOT delegate to insertAndGetAll. This helper uses createEntity.
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
    runAndWait(q.batchCreateEntities(workspaceId, entities, insertOnly = true)) shouldBe entities.size
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

  // helper to validate counts of entities by workspace and type. Note this does not have a `where deleted=0` clause.
  def getRawCounts: Seq[(UUID, String, Int)] = {
    import driver.api._ // for bespoke SQL queries
    implicit val getter: GetResult[(UUID, String, Int)] = GetResult(r => (r.<<, r.<<, r.<<))
    runAndWait(
      sql"select workspace_id, entity_type, count(1) from ENTITY group by workspace_id, entity_type"
        .as[(UUID, String, Int)]
    )
  }
}
