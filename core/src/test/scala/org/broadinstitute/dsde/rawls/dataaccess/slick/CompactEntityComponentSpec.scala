package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeEntityReferenceList, AttributeName, AttributeNumber, AttributeString, Entity}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID

class CompactEntityComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val q = compactEntityQuery

  behavior of "CompactEntityComponent"

  // tests both createEntity and getEntity
  it should "createEntity and getEntity with no attributes" in withMinimalTestDatabase { _ =>
    val entity = Entity("entityName", "entityType", Map())
    insertAndGet(entity)
  }

  it should "createEntity and getEntity with simple attributes" in withMinimalTestDatabase { _ =>
    val entity = Entity("entityName",
                        "entityType",
                        Map(
                          AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                          AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                        )
    )
    insertAndGet(entity)
  }
  it should "createEntity and getEntity with references" in withMinimalTestDatabase { _ =>
    // insert the rows being referenced
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

  behavior of "deleteReferences"

  behavior of "upsertReferences"

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
