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

class CompactEntityKeysCacheSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val ws2id = minimalTestData.workspace2.workspaceIdAsUUID
  private val q = compactEntityQuery

  // some attribute names used in tests
  private val attr1 = AttributeName.withDefaultNS("one")
  private val attr2 = AttributeName.withLibraryNS("two")
  private val attr3 = AttributeName.fromDelimitedName("three")
  private val attr4 = AttributeName.fromDelimitedName("namespace:four")
  private val attr5 = AttributeName.fromDelimitedName("other:five")
  private val attr6 = AttributeName.fromDelimitedName("six:six")

  behavior of "entity keys cache"

  it should "save and retrieve entity keys cache" in withMinimalTestDatabase { _ =>
    val entityType = "entityType"
    val keys = Set(attr1, attr2, attr3, attr4, attr5, attr6)

    // save the cache
    runAndWait(q.saveCache(wsid, entityType, keys)) shouldBe 1

    // retrieve the cache
    val actual = runAndWait(q.getCachedKeys(wsid))
    actual should contain only EntityTypeAndAttributeKeys(entityType, keys)
  }

  it should "save, update, and retrieve" is pending

  it should "save, invalidate, and retrieve" is pending

  it should "respect workspace boundaries" is pending

  it should "respect entityType criteria when one is supplied" is pending

  it should "respect entityType criteria when multiple are supplied" is pending

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
    runAndWait(q.batchWriteEntities(workspaceId, entities, insertOnly = true)) shouldBe entities.size
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
