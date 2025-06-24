package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization.{SqlEntityData, SqlEntityReference}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeBoolean,
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

  behavior of "getReferencesFrom"

  it should "insert and delete all" in withMinimalTestDatabase { _ =>
    // entities being referenced: the "targets"
    val target1 = Entity(s"toName1", "toType", Map())
    val target2 = Entity(s"toName2", "toType", Map())
    val target3 = Entity(s"toName3", "toType", Map())
    val target4 = Entity(s"toName4", "toType", Map())
    val target5 = Entity(s"toName5", "toType", Map())

    val allTargets = Seq(target1, target2, target3, target4, target5)

    // the entity doing the referencing: the "source"
    val targetRefs = allTargets.map { target =>
      AttributeEntityReference(target.entityType, target.name)
    }
    val sourceEntity = Entity("fromName",
                              "fromType",
                              Map(
                                AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(targetRefs)
                              )
    )

    // insert targets
    allTargets.foreach { target =>
      insertAndGet(target)
    }

    // source should have no rows in ENTITY_REFS table b/c we haven't inserted it yet
    runAndWait(q.getReferencesFrom(wsid, sourceEntity.toPointer)) shouldBe empty
    // insert the source
    insertAndGet(sourceEntity)
    runAndWait(q.getReferencesFrom(wsid, sourceEntity.toPointer)) should contain theSameElementsAs allTargets.map(
      _.toPointer
    )
    // delete the source
    runAndWait(q.deleteEntities(wsid, Seq(sourceEntity.toPointer)))
    runAndWait(q.getReferencesFrom(wsid, sourceEntity.toPointer)) shouldBe empty
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

    // insert targets
    (tos1 ++ tos2).toSet[EntityPointer].foreach { targetPointer =>
      insertAndGet(Entity(targetPointer.entityName, targetPointer.entityType, Map()))
    }
    // insert from1 with references of tos1
    val tos1Refs = tos1.map { target =>
      AttributeEntityReference(target.entityType, target.entityName)
    }
    insertAndGet(
      Entity(from1.entityName,
             from1.entityType,
             Map(
               AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(tos1Refs)
             )
      )
    )
    // insert from2 with references of tos2
    val tos2Refs = tos2.map { target =>
      AttributeEntityReference(target.entityType, target.entityName)
    }
    insertAndGet(
      Entity(from2.entityName,
             from2.entityType,
             Map(
               AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(tos2Refs)
             )
      )
    )

    runAndWait(q.getReferencesFrom(wsid, from1)) should contain theSameElementsAs tos1
    runAndWait(q.getReferencesFrom(wsid, from2)) should contain theSameElementsAs tos2
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

  behavior of "queryRelatedRecordsWithArray"

  it should "get the record for a single reference" in withMinimalTestDatabase { _ =>
    // Insert referenced entity
    val sample = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    // We'll need this later for comparison
    val insertedSample = insertAndGet(sample)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "sample1"))
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        wsid,
        "sample_set",
        "set1",
        List("samples")
      )
    )
    // TODO why is the list in an option?
    result.get(sample.name).get should contain(insertedSample)
  }

  it should "get the records for a list of references" in withMinimalTestDatabase { _ =>
    // Insert referenced entities
    val sample1 = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    val sample2 = Entity(
      "sample2",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    // TODO if i do insertAndGetAll is there a convenient way to do the comparison
    // We'll need these later for comparison
    val insertedSample1 = insertAndGet(sample1)
    val insertedSample2 = insertAndGet(sample2)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          List(AttributeEntityReference("sample", "sample1"), AttributeEntityReference("sample", "sample2"))
        )
      )
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "samples"
        )
      )
    )
    result.get(sample1.name).get should contain(insertedSample1)
    result.get(sample2.name).get should contain(insertedSample2)
  }

  it should "get the record for a chain of references" in withMinimalTestDatabase { _ =>
    // sample_set -> sample -> participant

    val participant = Entity(
      "p1",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    val insertedParticipant = insertAndGet(participant)

    val sample = Entity(
      "s1",
      "sample",
      Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "p1"))
    )

    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "s1"))
    )

    insertAndGetAll(Seq(sample, set))

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "samples",
          "participant"
        )
      )
    )
    result.get(sample.name).get should contain(insertedParticipant)
  }

  it should "get the records for a chain of references with an array" in withMinimalTestDatabase { _ =>
    // sample_set -> sample -> participant

    val participant1 = Entity(
      "p1",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    val participant2 = Entity(
      "p2",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    val insertedParticipant1 = insertAndGet(participant1)
    val insertedParticipant2 = insertAndGet(participant2)

    val sample1 = Entity(
      "s1",
      "sample",
      Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "p1"))
    )

    val sample2 = Entity(
      "s2",
      "sample",
      Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "p2"))
    )

    val set = Entity(
      "set1",
      "sample_set",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          List(AttributeEntityReference("sample", "s1"), AttributeEntityReference("sample", "s2"))
        )
      )
    )

    insertAndGetAll(Seq(sample1, sample2, set))

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "samples",
          "participant"
        )
      )
    )
    result.get(sample1.name).get should contain(insertedParticipant1)
    result.get(sample2.name).get should contain(insertedParticipant2)
  }

  it should "get the records for a chain of references with multiple arrays" in withMinimalTestDatabase { _ =>
    // sample_set -> sample -> participant

    val participant1 = Entity(
      "p1",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    val participant2 = Entity(
      "p2",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    val participant3 = Entity(
      "p3",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("c"))
    )

    val participant4 = Entity(
      "p4",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("d"))
    )

    val insertedParticipant1 = insertAndGet(participant1)
    val insertedParticipant2 = insertAndGet(participant2)
    val insertedParticipant3 = insertAndGet(participant3)
    val insertedParticipant4 = insertAndGet(participant4)

    val sample1 = Entity(
      "s1",
      "sample",
      Map(
        AttributeName.withDefaultNS("participant") -> AttributeEntityReferenceList(
          List(AttributeEntityReference("participant", "p1"), AttributeEntityReference("participant", "p3"))
        )
      )
    )

    val sample2 = Entity(
      "s2",
      "sample",
      Map(
        AttributeName.withDefaultNS("participant") -> AttributeEntityReferenceList(
          List(AttributeEntityReference("participant", "p2"), AttributeEntityReference("participant", "p4"))
        )
      )
    )

    val set = Entity(
      "set1",
      "sample_set",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          List(AttributeEntityReference("sample", "s1"), AttributeEntityReference("sample", "s2"))
        )
      )
    )

    insertAndGetAll(Seq(sample1, sample2, set))

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "samples",
          "participant"
        )
      )
    )
    result.get(sample1.name).get should contain theSameElementsAs Seq(insertedParticipant1, insertedParticipant3)
    result.get(sample2.name).get should contain theSameElementsAs Seq(insertedParticipant2, insertedParticipant4)
//    result.get(participant3.name) should equal(Some(insertedParticipant3))
//    result.get(participant4.name) should equal(Some(insertedParticipant4))
  }

  it should "only get records from the given workspace" in withMinimalTestDatabase { _ =>
    // Insert referenced entity
    val sample1 = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    // Entity of same name/type in a different workspace
    val sample2 = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    // We'll need this later for comparison
    val insertedSampleWS1 = insertAndGet(sample1)
    insertAndGet(sample2, minimalTestData.workspace2.workspaceIdAsUUID)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "sample1"))
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        wsid,
        "sample_set",
        "set1",
        List("samples")
      )
    )
    result.get(sample1.name).get should contain(insertedSampleWS1)
  }

  // TODO double check these tests are testing what they should
  it should "be case-sensitive on entity type" in withMinimalTestDatabase { _ =>
    // Insert referenced entity
    val sample = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    // We'll need this later for comparison
    val insertedSample = insertAndGet(sample)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "sample1"))
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        wsid,
        "Sample_set",
        "set1",
        List("samples")
      )
    )
    result.get(sample.name) shouldBe None
  }

  it should "be case-sensitive on attribute names" in withMinimalTestDatabase { _ =>
    // sample_set -> sample -> participant

    val participant = Entity(
      "p1",
      "participant",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("b"))
    )

    val sample = Entity(
      "s1",
      "sample",
      Map(AttributeName.withDefaultNS("participant") -> AttributeEntityReference("participant", "p1"))
    )

    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "s1"))
    )

    insertAndGetAll(Seq(sample, set, participant))

    val result1 = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "Samples",
          "participant"
        )
      )
    )
    result1.get(participant.name) shouldBe None

    val result2 = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "Samples",
          "Participant"
        )
      )
    )
    result2.get(participant.name) shouldBe None

    val result3 = runAndWait(
      q.queryRelatedRecordsWithArray(
        minimalTestData.workspace.workspaceIdAsUUID,
        "sample_set",
        "set1",
        List(
          "samples",
          "Participant"
        )
      )
    )
    result3.get(participant.name) shouldBe None
  }

  it should "be case-insensitive on entity name" in withMinimalTestDatabase { _ =>
    // Insert referenced entity
    val sample = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    // We'll need this later for comparison
    val insertedSample = insertAndGet(sample)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReference("sample", "sample1"))
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        wsid,
        "sample_set",
        "Set1",
        List("samples")
      )
    )
    result.get(sample.name).get should contain(insertedSample)
  }

  it should "handle non-default namespaces in attribute names" in withMinimalTestDatabase { _ =>
    // Insert referenced entity
    val sample = Entity(
      "sample1",
      "sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("a"))
    )

    // We'll need this later for comparison
    val insertedSample = insertAndGet(sample)

    // Referencing entity
    val set = Entity(
      "set1",
      "sample_set",
      Map(AttributeName("set_namespace", "samples") -> AttributeEntityReference("sample", "sample1"))
    )

    insertAndGet(set)

    val result = runAndWait(
      q.queryRelatedRecordsWithArray(
        wsid,
        "sample_set",
        "set1",
        List("set_namespace:samples")
      )
    )
    result.get(sample.name).get should contain(insertedSample)
  }

  behavior of "listEntityKeysViaEntity"

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

    val actual = runAndWait(q.listEntityKeysViaEntity(wsid))
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

  it should "update both $.attrs and $.refs in the entity" in withMinimalTestDatabase { _ =>
    val attr1 = AttributeName.withDefaultNS("foo")
    val attr2 = AttributeName.fromDelimitedName("import:bar")
    val attr3 = AttributeName.fromDelimitedName("library:baz")
    val attr4 = AttributeName.withDefaultNS("qux")

    // insert some reference targets
    val targets = (1 to 5).map { idx =>
      Entity(s"target$idx", "targetType", Map())
    }
    insertAndGetAll(targets)

    val entity1 = Entity(
      "entityName1",
      "entityType",
      Map(
        attr1 -> AttributeNumber(1),
        attr2 -> AttributeString("not a reference"),
        attr3 -> AttributeEntityReference("targetType", "target1"),
        attr4 -> AttributeEntityReferenceList(
          Seq(
            // note the "random" order of target3, target2, target 4 here
            AttributeEntityReference("targetType", "target3"),
            AttributeEntityReference("targetType", "target2"),
            AttributeEntityReference("targetType", "target4")
          )
        )
      )
    )

    insertAndGet(entity1)

    val expectedInitialRefs = Seq(
      SqlEntityReference(a = "library:baz", n = "target1", t = "targetType", z = Some(true)),
      SqlEntityReference(a = "qux", n = "target3", t = "targetType", z = None),
      SqlEntityReference(a = "qux", n = "target2", t = "targetType", z = None),
      SqlEntityReference(a = "qux", n = "target4", t = "targetType", z = None)
    )

    val initialActual = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    initialActual should not be empty
    initialActual.get.attributes should not be empty
    val initialActualData = initialActual.get.attributes.get.parseJson
      .convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    initialActualData.attrs.keys should contain theSameElementsAs Seq(attr1, attr2, attr3, attr4)
    initialActualData.refs should contain theSameElementsAs expectedInitialRefs
    // verify order for attr "qux" in refs
    initialActualData.refs.filter(_.a == "qux") should contain theSameElementsInOrderAs expectedInitialRefs.filter(
      _.a == "qux"
    )

    // delete attribute attr1 "foo"
    runAndWait(q.deleteAttributes(wsid, "entityType", Set(attr1)))

    val nextActual = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    nextActual should not be empty
    nextActual.get.attributes should not be empty
    val nextActualData =
      nextActual.get.attributes.get.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    nextActualData.attrs.keys should contain theSameElementsAs Seq(attr2, attr3, attr4)
    nextActualData.refs should contain theSameElementsAs expectedInitialRefs
    // verify order for attr "qux" in refs
    nextActualData.refs.filter(_.a == "qux") should contain theSameElementsInOrderAs expectedInitialRefs.filter(
      _.a == "qux"
    )

    // delete attribute attr3 "library:baz"
    runAndWait(q.deleteAttributes(wsid, "entityType", Set(attr3)))

    val moreActual = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    moreActual should not be empty
    moreActual.get.attributes should not be empty
    val moreActualData =
      moreActual.get.attributes.get.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    moreActualData.attrs.keys should contain theSameElementsAs Seq(attr2, attr4)
    // refs should now only contain "qux"
    moreActualData.refs should contain theSameElementsInOrderAs expectedInitialRefs.filter(_.a == "qux")

    // delete attribute attr4 "qux"
    runAndWait(q.deleteAttributes(wsid, "entityType", Set(attr4)))

    val lastActual = runAndWait(q.getEntity(wsid, entity1.entityType, entity1.name))
    lastActual should not be empty
    lastActual.get.attributes should not be empty
    val lastActualData =
      lastActual.get.attributes.get.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    lastActualData.attrs.keys should contain theSameElementsAs Seq(attr2)
    // refs should now be empty
    lastActualData.refs shouldBe empty
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

  it should "update both $.attrs and $.refs" in withMinimalTestDatabase { _ =>
    // Create target entities that will be referenced
    val target1 = Entity("targetName1", "targetType", Map())
    val target2 = Entity("targetName2", "targetType", Map())
    insertAndGetAll(Seq(target1, target2))

    // Create entity with a simple attribute
    val originalAttributeName = AttributeName.withDefaultNS("originalAttr")
    val newAttributeName = AttributeName.withDefaultNS("newAttr")
    val entityWithAttribute = Entity(
      "entityWithAttribute",
      "entityType",
      Map(originalAttributeName -> AttributeString("attributeValue"))
    )

    // Create entity with a reference using the same attribute name
    val entityWithReference = Entity(
      "entityWithReference",
      "entityType",
      Map(originalAttributeName -> AttributeEntityReference("targetType", "targetName1"))
    )

    // Create entity with a reference list using the same attribute name
    val entityWithReferenceList = Entity(
      "entityWithReferenceList",
      "entityType",
      Map(
        originalAttributeName -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("targetType", "targetName1"),
            AttributeEntityReference("targetType", "targetName2")
          )
        )
      )
    )

    insertAndGetAll(Seq(entityWithAttribute, entityWithReference, entityWithReferenceList))

    // Rename the attribute
    runAndWait(
      q.renameAttribute(wsid, "entityType", originalAttributeName, AttributeRename(newAttributeName))
    ) shouldBe 3

    // Verify the attribute was renamed in all entities
    for (entityName <- Seq("entityWithAttribute", "entityWithReference", "entityWithReferenceList")) {
      val updatedEntity = runAndWait(q.getEntity(wsid, "entityType", entityName)).get.toEntity
      updatedEntity.attributes should contain key newAttributeName
      updatedEntity.attributes.contains(originalAttributeName) shouldBe false
    }

    // Verify the raw JSON structure of the entity with reference list
    val entityWithRefList = runAndWait(q.getEntity(wsid, "entityType", "entityWithReferenceList"))
    entityWithRefList should not be empty

    val rawJson = entityWithRefList.get.attributes.get
    val entityData = rawJson.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)

    // Check that attrs section has the new attribute name
    entityData.attrs.keys should contain(newAttributeName)
    entityData.attrs.keys should not contain originalAttributeName

    // Check that refs section has the new attribute name
    entityData.refs.exists(_.a == toDelimitedName(newAttributeName)) shouldBe true
    entityData.refs.exists(_.a == toDelimitedName(originalAttributeName)) shouldBe false

    // Verify that the original attribute name doesn't appear anywhere in the JSON string
    val originalAttrDelimited = toDelimitedName(originalAttributeName)

    // Check for attribute name in JSON with space after colon (standard JSON format)
    rawJson should not include s""""a": "$originalAttrDelimited""""

    // Check for attribute name in JSON without space after colon (compact format)
    rawJson should not include s""""a":"$originalAttrDelimited""""

    // Check raw JSON includes the new attribute name with proper spacing
    val newAttrDelimited = toDelimitedName(newAttributeName)
    rawJson should include regex s""""a":\\s*"$newAttrDelimited""""
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
    val allTargets: Seq[Entity] = Seq(target1, target2, target3, target4, target5, target6).map { targetPointer =>
      Entity(targetPointer.entityName, targetPointer.entityType, Map())
    }
    insertAndGetAll(allTargets)
    val allSources: Seq[Entity] = Seq(pointers1, pointers2, pointers3).map { refMapping =>
      Entity(
        refMapping.from.entityName,
        refMapping.from.entityType,
        Map(
          AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
            refMapping.to.map { target =>
              AttributeEntityReference(target.entityType, target.entityName)
            }.toSeq
          )
        )
      )
    }
    insertAndGetAll(allSources)

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
    val target1 = Entity("targetName1", "targetType", Map())

    // insert rows for workspace 1: source1 has a reference to target1, source2 does not
    insertAndGetAll(
      Seq(
        target1,
        Entity(
          source1.entityName,
          source1.entityType,
          Map(
            AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
              Seq(AttributeEntityReference("targetType", "targetName1"))
            )
          )
        ),
        Entity(source2.entityName, source2.entityType, Map())
      ),
      wsid
    )

    // insert rows for workspace 2: source2 has a reference to target1, source1 does not
    insertAndGetAll(
      Seq(
        target1,
        Entity(
          source1.entityName,
          source1.entityType,
          Map(
          )
        ),
        Entity(
          source2.entityName,
          source2.entityType,
          Map(
            AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
              Seq(AttributeEntityReference("targetType", "targetName1"))
            )
          )
        )
      ),
      wsid2
    )

    runAndWait(q.getReferencesTo(wsid, Seq(target1.toPointer))) should contain theSameElementsAs Seq(source1)
    runAndWait(q.getReferencesTo(wsid2, Seq(target1.toPointer))) should contain theSameElementsAs Seq(source2)
  }

  it should "exclude entities included in the search" in withMinimalTestDatabase { _ =>
    // define entities
    // entity2 references entity1; entity3 references both entity1 and entity2
    val entity1 = Entity("name1", "entityType", Map())
    val entity2 =
      Entity("name2",
             "entityType",
             Map(
               AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity1.entityType, entity1.name)
             )
      )
    val entity3 = Entity(
      "name3",
      "entityType",
      Map(
        AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity1.entityType, entity1.name),
        AttributeName.withDefaultNS("ref2") -> AttributeEntityReference(entity2.entityType, entity2.name)
      )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    // asking for references to entity1 and entity2 should exclude entity2 because entity2 is in the search criteria,
    //  even though entity2 references entity1
    val expected = Set(entity3.toPointer)
    runAndWait(
      q.getReferencesTo(wsid, Seq(entity1.toPointer, entity2.toPointer))
    ).toSet should contain theSameElementsAs expected

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

    // insert targets
    insertAndGetAll(
      Seq(target1, target2, target3).map { targetPointer =>
        Entity(targetPointer.entityName, targetPointer.entityType, Map())
      }
    )
    // insert sources and their references to the targets
    val sourcesWithReferences = Seq(pointers1, pointers2, pointers3).map { refMapping =>
      Entity(
        refMapping.from.entityName,
        refMapping.from.entityType,
        Map(
          AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
            refMapping.to.map { target =>
              AttributeEntityReference(target.entityType, target.entityName)
            }.toSeq
          )
        )
      )
    }
    insertAndGetAll(sourcesWithReferences)

    val expected = Set(source2, source3)
    runAndWait(q.getReferencesToType(wsid, targetType1)) should contain theSameElementsAs expected
  }

  it should "not return source entities of the same type" in withMinimalTestDatabase { _ =>
    // define entities
    // entity2 references entity1; entity3 references both entity1 and entity2
    // entity3 has a different entity type
    val entity1 = Entity("name1", "entityTypeA", Map())
    val entity2 =
      Entity("name2",
             "entityTypeA",
             Map(
               AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity1.entityType, entity1.name)
             )
      )
    val entity3 = Entity(
      "name3",
      "entityTypeB",
      Map(
        AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity1.entityType, entity1.name),
        AttributeName.withDefaultNS("ref2") -> AttributeEntityReference(entity2.entityType, entity2.name)
      )
    )

    insertAndGetAll(Seq(entity1, entity2, entity3))

    // asking for references to entity1 should exclude entity2 because entity2 is of the same type,
    //  even though entity2 references entity1
    runAndWait(q.getReferencesToType(wsid, "entityTypeA")).toSet should contain theSameElementsAs Set(entity3.toPointer)

  }

  private val columnFilterCases = List(
    (AttributeNumber(42), "42"),
    (AttributeString("foo"), "foo"),
    (AttributeString("fOo"), "foO"), // case sensitivity check
    (AttributeBoolean(true), "TRUE")
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

  it should "rename in both $.attrs and $.refs when updating references" in withMinimalTestDatabase { _ =>
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

    // Rename the entity
    val newName = "newName"
    runAndWait(q.renameEntity(wsid, entityType, originalEntity.name, newName)) shouldBe 1

    // Verify the entity was renamed
    val renamedEntity = runAndWait(q.getEntity(wsid, entityType, newName)).get.toEntity
    renamedEntity.name shouldBe newName

    // verify that "originalName" is now "newName" in $.attrs for sourceEntity2
    val source2 = runAndWait(q.getEntity(wsid, "sourceType", "sourceEntity2"))
    source2 should not be empty
    source2.get.attributes should not be empty
    val rawData2 =
      source2.get.attributes.get.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    rawData2.attrs(AttributeName.withDefaultNS("ref")) shouldBe AttributeString("newName")

    // N.B. sourceEntity1 has a reference list, so it doesn't get the rename in $.attrs
    // verify that $.attrs for sourceEntity1 is still `1`
    val source1 = runAndWait(q.getEntity(wsid, "sourceType", "sourceEntity1"))
    source1 should not be empty
    source1.get.attributes should not be empty
    val rawData1 =
      source1.get.attributes.get.parseJson.convertTo[SqlEntityData](CompactEntitySerialization.sqlEntityDataFormat)
    rawData1.attrs(AttributeName.withDefaultNS("refList")) shouldBe AttributeNumber(1)

  }

  it should "respect entity type when renaming" in withMinimalTestDatabase { _ =>
    // Create the original entity
    val entityType = "testType"
    val originalEntity = Entity("originalName", entityType, Map())
    insertAndGet(originalEntity)

    // Create another entity with the same name but a different type
    val otherEntityType = s"$entityType-other"
    val otherEntity = Entity(originalEntity.name, otherEntityType, Map())
    insertAndGet(otherEntity)

    // Rename the entity
    val newName = "newName"
    runAndWait(q.renameEntity(wsid, entityType, originalEntity.name, newName)) shouldBe 1

    // Verify the entity was renamed
    val renamedEntity = runAndWait(q.getEntity(wsid, entityType, newName)).get.toEntity
    renamedEntity.name shouldBe newName

    // Verify the old name no longer exists
    runAndWait(q.getEntity(wsid, entityType, originalEntity.name)) shouldBe None

    // Verify the other entity still exists
    val otherEntityLookup = runAndWait(q.getEntity(wsid, otherEntity.entityType, otherEntity.name)).get.toEntity
    otherEntityLookup shouldBe otherEntity

    // Verify nothing exists at the other entity's type plus the new name
    val otherEntityRenamedLookup = runAndWait(q.getEntity(wsid, otherEntity.entityType, newName))
    otherEntityRenamedLookup shouldBe None
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

  it should "not change non references that match the old name" in withMinimalTestDatabase { _ =>
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
      (1 to 10000).map(_ => targetEntity.toReference)
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

  it should "get recursive entity references" in withMinimalTestDatabase { _ =>
    // Define workspace
    val workspaceId = minimalTestData.workspace.workspaceIdAsUUID

    // Define references to form a recursive structure
    val entity4 = Entity("entityName4", "entityType1", Map())
    val entity3 =
      Entity("entityName3",
             "entityType1",
             Map(
               AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity4.entityType, entity4.name)
             )
      )
    val entity2 =
      Entity("entityName2",
             "entityType1",
             Map(
               AttributeName.withDefaultNS("ref1") -> AttributeEntityReference(entity4.entityType, entity4.name)
             )
      )
    val entity1 = Entity(
      "entityName1",
      "entityType1",
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference(entity2.entityType, entity2.name),
            AttributeEntityReference(entity3.entityType, entity3.name)
          )
        )
      )
    )

    // insert all entities
    insertAndGetAll(Seq(entity4, entity3, entity2, entity1))

    // Perform the recursive query
    val recursiveReferences = runAndWait(q.recursiveGetEntityReferences(workspaceId, Set(entity1.toPointer)))

    // Verify the result
    recursiveReferences should contain theSameElementsAs Set(
      RefMapping(entity1.toPointer, Set(entity2.toPointer, entity3.toPointer)),
      RefMapping(entity2.toPointer, Set(entity4.toPointer)),
      RefMapping(entity3.toPointer, Set(entity4.toPointer))
    )
  }

  it should "get recursive entity references with cycles" in withMinimalTestDatabase { _ =>
    // Define workspace
    val workspaceId = minimalTestData.workspace.workspaceIdAsUUID

    // Create entities
    val entity1 = Entity("entityName1", "entityType1", Map())
    val entity2 = Entity("entityName2", "entityType1", Map())
    insertAndGetAll(Seq(entity1, entity2))

    // Update the entities to contain a cycle
    val entity1Update = entity1.copy(attributes =
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(entity2.entityType, entity2.name)
      )
    )
    val entity2Update = entity2.copy(attributes =
      Map(
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference(entity1.entityType, entity1.name)
      )
    )
    runAndWait(q.batchCreateEntities(workspaceId, Seq(entity1Update, entity2Update), insertOnly = false))

    // Perform the recursive query
    val recursiveReferences = runAndWait(q.recursiveGetEntityReferences(workspaceId, Set(entity1.toPointer)))

    // Verify the result
    recursiveReferences should contain theSameElementsAs Set(
      RefMapping(entity1.toPointer, Set(entity2.toPointer)),
      RefMapping(entity2.toPointer, Set(entity1.toPointer))
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
