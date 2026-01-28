package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.{Sink, Source}
import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddListMember,
  AddUpdateAttribute,
  AttributeUpdateOperation,
  CreateAttributeEntityReferenceList,
  EntityUpdateDefinition,
  RemoveAttribute
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeRename,
  AttributeString,
  AttributeValueList,
  Entity,
  EntityPointer,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}

import java.sql.SQLException
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * This spec tests from CompactEntityProvider down to the database.
  *
  * Compare to CompactEntityProviderSpec for tests that don't hit the database.
  *
  */
class CompactEntityProviderE2ESpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val q = compactEntityQuery

  private val defaultRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  private val defaultEntityRequestArguments =
    EntityRequestArguments(minimalTestData.workspace, defaultRequestContext)

  private val atMost = Duration("60 seconds") // timeout for Await() in tests

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val system: ActorSystem = ActorSystem("CompactEntityProviderE2ESpec")

  behavior of "batchUpsertEntities"

  // Note that this class does not have extensive coverage of the various operations possible in a batchUpsert, such as
  // removing attributes or creating attribute lists. Those batch upsert features rely on
  // AttributeSupport.applyOperationsToEntity, which is tested elsewhere.

  it should "create entities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq()),
      EntityUpdateDefinition("name3", "typeB", Seq())
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 3

    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1
  }

  it should "create entities with references" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("ref"), AttributeEntityReference("typeA", "name1")))
      ),
      EntityUpdateDefinition(
        "name3",
        "typeB",
        Seq(
          AddUpdateAttribute(
            AttributeName.withDefaultNS("refs"),
            AttributeEntityReferenceList(
              Seq(
                AttributeEntityReference("typeA", "name1"),
                AttributeEntityReference("typeA", "name2")
              )
            )
          )
        )
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 3

    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    val ref1 = EntityPointer("typeA", "name1")
    val ref2 = EntityPointer("typeA", "name2")
    val ref3 = EntityPointer("typeB", "name3")

    // entity with "name1" should have no references
    runAndWait(q.getReferencesFrom(wsid, ref1)) shouldBe empty

    // entity with "name2" should have a reference to "name1"
    runAndWait(q.getReferencesFrom(wsid, ref2)) shouldBe Seq(ref1)

    // entity with "name3" should have references to both "name1" and "name2"
    runAndWait(q.getReferencesFrom(wsid, ref3)) should contain theSameElementsAs Seq(
      ref1,
      ref2
    )

  }

  it should "update entities with references" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // create target entities
    Await.result(provider.createEntity(Entity("targetName1", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName2", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName3", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName4", "targetType", Map()), defaultRequestContext), atMost)

    // create the entity we'll be updating; give it two references
    Await.result(
      provider.createEntity(
        Entity(
          "sourceName",
          "sourceType",
          Map(
            AttributeName.withDefaultNS("ref1") -> AttributeEntityReference("targetType", "targetName1"),
            AttributeName.withDefaultNS("ref2") -> AttributeEntityReference("targetType", "targetName2")
          )
        ),
        defaultRequestContext
      ),
      atMost
    )

    // validate the starting references before our batchUpsert
    val initialReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    initialReferences shouldBe Seq(EntityPointer("targetType", "targetName1"),
                                   EntityPointer("targetType", "targetName2")
    )

    // perform the batchUpsert - delete one existing reference, add two more
    val updates = Source(
      Seq(
        EntityUpdateDefinition(
          "sourceName",
          "sourceType",
          Seq(
            CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refList")),
            AddListMember(AttributeName.withDefaultNS("refList"),
                          AttributeEntityReference("targetType", "targetName3")
            ),
            AddListMember(AttributeName.withDefaultNS("refList"), AttributeEntityReference("targetType", "targetName4"))
          )
        ),
        EntityUpdateDefinition("sourceName",
                               "sourceType",
                               Seq(
                                 RemoveAttribute(AttributeName.withDefaultNS("ref2"))
                               )
        )
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(updates, defaultRequestContext), atMost)
    numUpdated shouldBe 1

    // validate the references after our batchUpsert
    val finalReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    finalReferences.toSet shouldBe Set(
      EntityPointer("targetType", "targetName1"),
      EntityPointer("targetType", "targetName3"),
      EntityPointer("targetType", "targetName4")
    )

  }

  it should "succeed for a noop update" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    val testEntity = Entity(
      "myName",
      "myType",
      Map(
        AttributeName.withDefaultNS("one") -> AttributeNumber(1),
        AttributeName.withDefaultNS("two") -> AttributeNumber(2)
      )
    )

    // create the entity we'll be updating; give it two references
    Await.result(
      provider.createEntity(testEntity, defaultRequestContext),
      atMost
    )

    // perform the batchUpsert, issuing a noop update
    val updates = Source(
      Seq(
        EntityUpdateDefinition(
          "myName",
          "myType",
          Seq(
            AddUpdateAttribute(AttributeName.withDefaultNS("one"), AttributeNumber(1))
          )
        )
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(updates, defaultRequestContext), atMost)
    numUpdated shouldBe 0

    // validate the entity after our batchUpsert
    val finalEntity =
      runAndWait(
        provider.repository.queries.getEntity(wsid, "myType", "myName")
      )
    finalEntity should not be empty
    finalEntity.get.toEntity shouldBe testEntity
  }

  it should "remove obsolete references for entities during update" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // create target entity
    Await.result(provider.createEntity(Entity("targetName", "targetType", Map()), defaultRequestContext), atMost)

    // create the entity we'll be updating; give it a reference
    Await.result(
      provider.createEntity(
        Entity("sourceName",
               "sourceType",
               Map(AttributeName.withDefaultNS("ref") -> AttributeEntityReference("targetType", "targetName"))
        ),
        defaultRequestContext
      ),
      atMost
    )

    // validate the starting reference before our batchUpsert
    val initialReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    initialReferences shouldBe Seq(EntityPointer("targetType", "targetName"))

    // perform the batchUpsert - delete the existing reference, don't add any
    val updates = Source(
      Seq(
        EntityUpdateDefinition("sourceName", "sourceType", Seq(RemoveAttribute(AttributeName.withDefaultNS("ref"))))
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(updates, defaultRequestContext), atMost)
    numUpdated shouldBe 1

    // validate the references after our batchUpsert
    val finalReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    finalReferences.toSet shouldBe empty

  }

  it should "create entities across multiple batches" in withMinimalTestDatabase { _ =>
    val repository = new CompactEntityRepository(slickDataSource)
    val config = CompactEntityProviderConfig(batchUpsertBatchSize = 250) // pretty small to force batching

    val provider =
      new CompactEntityProvider(defaultEntityRequestArguments, repository, "testMetricPrefix", config)(ec, system)

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // define 1000 entities, each with a text attribute ranging from 8 to 24 bytes
    val updates: Seq[EntityUpdateDefinition] = Range(0, 1000) map { idx =>
      EntityUpdateDefinition(s"name$idx",
                             "typeA",
                             Seq(
                               AddUpdateAttribute(AttributeName.withDefaultNS("sometext"),
                                                  AttributeString(idx.toString * 8)
                               )
                             )
      )
    }

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 1000

    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 1
    metadataAfter.keys should contain theSameElementsAs Seq("typeA")
    metadataAfter("typeA").count shouldBe 1000
  }

  it should "roll back all writes, even across batches, on error" in withMinimalTestDatabase { _ =>
    val repository = new CompactEntityRepository(slickDataSource)
    val config = CompactEntityProviderConfig(batchUpsertBatchSize = 1) // should execute one update per batch

    val provider =
      new CompactEntityProvider(defaultEntityRequestArguments, repository, "testMetricPrefix", config)(ec, system)

    // the fourth update in this list has an unsupported character in its name and will cause a SQL error
    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq()),
      EntityUpdateDefinition("name3", "typeA", Seq()),
      EntityUpdateDefinition("name\uD83D\uDE0E", "typeA", Seq())
    )

    // no entities should exist before the batchUpsert
    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // perform the batchUpsert
    intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    }

    // everything should be rolled back; no entities should exist after the upsert
    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter shouldBe empty
  }

  it should "create or update entities as appropriate" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // create two entities
    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1")))
      ),
      EntityUpdateDefinition("name3",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val3")))
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 2

    // cursory validation of the entity creation
    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 1
    metadataAfter.keys should contain theSameElementsAs Seq("typeA")
    metadataAfter("typeA").count shouldBe 2

    // now perform an upsert of 4 entities: 2 new, 2 updated. "name1" and "name3" were already created above.
    val upserts: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1, updated")))
      ),
      EntityUpdateDefinition("name2",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val2")))
      ),
      EntityUpdateDefinition(
        "name3",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val3, updated")))
      ),
      EntityUpdateDefinition("name4",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val4")))
      )
    )

    Await.result(provider.batchUpsertEntities(Source(upserts), defaultRequestContext), atMost)

    // validate the upserts
    val metadataAfterUpsert = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfterUpsert.size shouldBe 1
    metadataAfterUpsert.keys should contain theSameElementsAs Seq("typeA")
    metadataAfterUpsert("typeA").count shouldBe 4

    Await.result(provider.getEntity("typeA", "name1", defaultRequestContext), atMost) shouldBe Entity(
      "name1",
      "typeA",
      Map(
        AttributeName.withDefaultNS("col1") -> AttributeString("val1, updated")
      )
    )
    Await.result(provider.getEntity("typeA", "name2", defaultRequestContext), atMost) shouldBe Entity(
      "name2",
      "typeA",
      Map(
        AttributeName.withDefaultNS("col1") -> AttributeString("val2")
      )
    )
    Await.result(provider.getEntity("typeA", "name3", defaultRequestContext), atMost) shouldBe Entity(
      "name3",
      "typeA",
      Map(
        AttributeName.withDefaultNS("col1") -> AttributeString("val3, updated")
      )
    )
    Await.result(provider.getEntity("typeA", "name4", defaultRequestContext), atMost) shouldBe Entity(
      "name4",
      "typeA",
      Map(
        AttributeName.withDefaultNS("col1") -> AttributeString("val4")
      )
    )

  }

  it should "update entities multiple times if necessary" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // issue multiple updates to the same entity
    val updates = Range(1, 10).map { idx =>
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS(s"col$idx"), AttributeString(s"val$idx")))
      )
    }

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    val actual = Await.result(provider.getEntity("typeA", "name1", defaultRequestContext), atMost)

    val expectedAttributes = Range(1, 10).map { idx =>
      AttributeName.withDefaultNS(s"col$idx") -> AttributeString(s"val$idx")
    }.toMap

    actual shouldBe Entity("name1", "typeA", expectedAttributes)

  }

  it should "handle noop updates" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // create the pre-existing base entities
    val baseEntity1 = Entity("name1", "myType", Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
    val setup1 = Await.result(provider.createEntity(baseEntity1, defaultRequestContext), atMost)
    setup1 shouldBe baseEntity1

    val baseEntity2 = Entity("name2", "myType", Map(AttributeName.withDefaultNS("baz") -> AttributeString("qux")))
    val setup2 = Await.result(provider.createEntity(baseEntity2, defaultRequestContext), atMost)
    setup2 shouldBe baseEntity2

    // issue multiple updates to the same entity
    val updates = Seq(
      EntityUpdateDefinition(
        "name1",
        "myType",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("bar"))) // this is a noop update
      ),
      EntityUpdateDefinition(
        "name2",
        "myType",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("baz"), AttributeString("changed!"))) // this is an update
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 1 // only one entity was actually updated

    // entity1 remains unchanged
    val actual1 = Await.result(provider.getEntity("myType", "name1", defaultRequestContext), atMost)
    actual1 shouldBe baseEntity1

    // entity2 has been updated
    val actual2 = Await.result(provider.getEntity("myType", "name2", defaultRequestContext), atMost)
    actual2 shouldBe baseEntity2.copy(attributes =
      Map(AttributeName.withDefaultNS("baz") -> AttributeString("changed!"))
    )

  }

  it should "error for update-only on missing entities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    Await.result(provider.createEntity(Entity("name1", "typeA", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("name2", "typeA", Map()), defaultRequestContext), atMost)

    // batch update attempts to write to "name1" and "name3", which should fail
    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1")))
      ),
      EntityUpdateDefinition("name3",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val3")))
      )
    )

    intercept[EntityNotFoundException] {
      Await.result(provider.batchUpdateEntities(Source(updates), defaultRequestContext), atMost)
    }
  }

  behavior of "batchUpdateEntities"

  it should "update entities with references" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val metadataBefore = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataBefore shouldBe empty

    // create target entities
    Await.result(provider.createEntity(Entity("targetName1", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName2", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName3", "targetType", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("targetName4", "targetType", Map()), defaultRequestContext), atMost)

    // create the entity we'll be updating; give it two references
    Await.result(
      provider.createEntity(
        Entity(
          "sourceName",
          "sourceType",
          Map(
            AttributeName.withDefaultNS("ref1") -> AttributeEntityReference("targetType", "targetName1"),
            AttributeName.withDefaultNS("ref2") -> AttributeEntityReference("targetType", "targetName2")
          )
        ),
        defaultRequestContext
      ),
      atMost
    )

    // validate the starting references before our batchUpsert
    val initialReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    initialReferences shouldBe Seq(EntityPointer("targetType", "targetName1"),
                                   EntityPointer("targetType", "targetName2")
    )

    // perform the batchUpsert - delete one existing reference, add two more
    val updates = Source(
      Seq(
        EntityUpdateDefinition(
          "sourceName",
          "sourceType",
          Seq(
            CreateAttributeEntityReferenceList(AttributeName.withDefaultNS("refList")),
            AddListMember(AttributeName.withDefaultNS("refList"),
                          AttributeEntityReference("targetType", "targetName3")
            ),
            AddListMember(AttributeName.withDefaultNS("refList"), AttributeEntityReference("targetType", "targetName4"))
          )
        ),
        EntityUpdateDefinition("sourceName",
                               "sourceType",
                               Seq(
                                 RemoveAttribute(AttributeName.withDefaultNS("ref2"))
                               )
        )
      )
    )

    Await.result(provider.batchUpdateEntities(updates, defaultRequestContext), atMost)

    // validate the references after our batchUpsert
    val finalReferences =
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName"))
      )

    finalReferences.toSet shouldBe Set(
      EntityPointer("targetType", "targetName1"),
      EntityPointer("targetType", "targetName3"),
      EntityPointer("targetType", "targetName4")
    )

  }

  behavior of "listEntities"

  it should "list entities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create entities
    val updates = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq())
    )
    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    // List entities
    val entities = Await.result(
      provider.listEntities("typeA").runFold(Seq.empty[Entity])(_ :+ _),
      atMost
    )

    entities.map(_.name) should contain theSameElementsAs Seq("name1", "name2")
  }

  it should "list entities with attributes" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create entities with attributes
    val updates = Seq(
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("bar")))
      ),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("baz"), AttributeString("qux")))
      )
    )
    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    // List entities
    val entities = Await.result(
      provider.listEntities("typeA").runFold(Seq.empty[Entity])(_ :+ _),
      atMost
    )

    entities should contain theSameElementsAs Seq(
      Entity("name1", "typeA", Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"))),
      Entity("name2", "typeA", Map(AttributeName.withDefaultNS("baz") -> AttributeString("qux")))
    )
  }

  it should "list entities when none" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // List entities when none exist
    val entities = Await.result(
      provider.listEntities("typeA").runFold(Seq.empty[Entity])(_ :+ _),
      atMost
    )

    entities shouldBe empty
  }

  behavior of "updateEntity"

  it should "correctly update an entity in the database" in withMinimalTestDatabase { _ =>
    val entityType = "typeA"
    val entityName = "name1"
    val provider = defaultProvider()

    // create the pre-existing base entity
    val baseEntity = Entity(entityName, entityType, Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
    val setup = Await.result(provider.createEntity(baseEntity, defaultRequestContext), atMost)
    setup shouldBe baseEntity

    // ask to apply an update to that entity
    val operations: Seq[AttributeUpdateOperation] = Seq(
      AddUpdateAttribute(AttributeName.withDefaultNS("baz"), AttributeString("qux"))
    )
    val actual = Await.result(provider.updateEntity(entityType, entityName, operations, defaultRequestContext), atMost)

    actual shouldBe Entity(entityName,
                           entityType,
                           Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                               AttributeName.withDefaultNS("baz") -> AttributeString("qux")
                           )
    )

  }

  behavior of "renameEntity"

  it should "not rename a non-existent entity" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Attempt to rename a non-existent entity
    intercept[EntityNotFoundException] {
      Await.result(provider.renameEntity("typeA", "nonExistentName", "newName", defaultRequestContext), atMost)
    }
  }

  it should "not rename an entity to an existing name" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create two entities
    Await.result(provider.createEntity(Entity("entity1", "typeA", Map()), defaultRequestContext), atMost)
    Await.result(provider.createEntity(Entity("entity2", "typeA", Map()), defaultRequestContext), atMost)

    // Attempt to rename the first entity to the name of the second entity
    intercept[DataEntityException] {
      Await.result(provider.renameEntity("typeA", "entity1", "entity2", defaultRequestContext), atMost)
    }
  }

  it should "rename an entity without references" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create an entity
    val originalEntity = Entity("oldName", "typeA", Map(AttributeName.withDefaultNS("foo") -> AttributeString("bar")))
    Await.result(provider.createEntity(originalEntity, defaultRequestContext), atMost)

    // Rename the entity
    val renamedEntity =
      Await.result(provider.renameEntity("typeA", "oldName", "newName", defaultRequestContext), atMost)

    // Validate the renamed entity
    renamedEntity shouldBe 1

    // Ensure the old name no longer exists
    intercept[EntityNotFoundException] {
      Await.result(provider.getEntity("typeA", "oldName", defaultRequestContext), atMost)
    }
  }

  it should "rename an entity with references" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create target entities
    val target1 = Entity("target1", "targetType", Map())
    val target2 = Entity("target2", "targetType", Map())
    Await.result(provider.createEntity(target1, defaultRequestContext), atMost)
    Await.result(provider.createEntity(target2, defaultRequestContext), atMost)

    // Create the entity to be renamed, with a reference
    val entityToRename = Entity(
      "entity1",
      "typeA",
      Map(AttributeName.withDefaultNS("ref") -> target1.toReference)
    )
    Await.result(provider.createEntity(entityToRename, defaultRequestContext), atMost)

    // Rename the entity
    val renamedEntity =
      Await.result(provider.renameEntity("typeA", entityToRename.name, "renamedEntity", defaultRequestContext), atMost)

    // Validate the renamed entity
    renamedEntity shouldBe 1

    // Ensure the old name no longer exists
    intercept[EntityNotFoundException] {
      Await.result(provider.getEntity("typeA", entityToRename.name, defaultRequestContext), atMost)
    }

    // Validate the reference still points to the correct target
    val updatedEntity = Await.result(provider.getEntity("typeA", "renamedEntity", defaultRequestContext), atMost)
    updatedEntity.attributes(AttributeName.withDefaultNS("ref")) shouldBe target1.toReference
  }

  it should "rename an entity that is a reference" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // Create the target entity to be renamed
    val targetEntity = Entity("targetEntity", "targetType", Map())
    Await.result(provider.createEntity(targetEntity, defaultRequestContext), atMost)

    // Create a referencing entity that points to the target entity
    val referencingEntity = Entity(
      "referencingEntity",
      "refType",
      Map(AttributeName.withDefaultNS("ref") -> targetEntity.toReference)
    )
    Await.result(provider.createEntity(referencingEntity, defaultRequestContext), atMost)

    // Rename the target entity
    val renamedEntity =
      Await.result(provider.renameEntity("targetType", "targetEntity", "renamedTarget", defaultRequestContext), atMost)

    // Validate the renamed entity
    renamedEntity shouldBe 1

    // Ensure the old name no longer exists
    intercept[EntityNotFoundException] {
      Await.result(provider.getEntity("targetType", "targetEntity", defaultRequestContext), atMost)
    }

    // Validate the reference in the referencing entity is updated
    val updatedReferencingEntity =
      Await.result(provider.getEntity("refType", "referencingEntity", defaultRequestContext), atMost)
    updatedReferencingEntity.attributes(AttributeName.withDefaultNS("ref")) shouldBe AttributeEntityReference(
      "targetType",
      "renamedTarget"
    )
  }

  behavior of "renameAttribute"

  it should "rename an attribute" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val oldAttr = AttributeName.withDefaultNS("foo")
    val newAttr = AttributeName.withDefaultNS("aFancyNewName")

    // Create entities with attributes
    val updates = Seq(
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(AddUpdateAttribute(oldAttr, AttributeNumber(1)))
      ),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("bar"), AttributeNumber(2)))
      ),
      EntityUpdateDefinition(
        "name3",
        "typeB",
        Seq(AddUpdateAttribute(oldAttr, AttributeNumber(3)))
      )
    )
    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    val numRenamed =
      Await.result(provider.renameAttribute("typeA", oldAttr, AttributeRename(newAttr), defaultRequestContext), atMost)
    numRenamed shouldBe 1

    val entitySource = provider.listEntities("typeA")
    val actual = Await.result(entitySource.runWith(Sink.seq), atMost)
    actual should contain theSameElementsAs Seq(
      Entity("name1", "typeA", Map(newAttr -> AttributeNumber(1))),
      Entity("name2", "typeA", Map(AttributeName.withDefaultNS("bar") -> AttributeNumber(2)))
    )

  }

  behavior of "deleteEntityAttributes"

  it should "delete attributes from entities" in withMinimalTestDatabase { _ =>
    // create providers for workspace1 and workspace2
    val repository = new CompactEntityRepository(slickDataSource)
    val ws1Provider =
      new CompactEntityProvider(defaultEntityRequestArguments, repository, "testMetricPrefix")(ec, system)
    val ws2Provider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = minimalTestData.workspace2),
      repository,
      "testMetricPrefix"
    )(ec, system)

    // define attributes
    val attrName1 = AttributeName.withDefaultNS("attr1")
    val attrName2 = AttributeName.withDefaultNS("attr2")
    val attrName3 = AttributeName.withDefaultNS("attr3")
    val attrName4 = AttributeName.withDefaultNS("attr4")
    val attrName5 = AttributeName.withDefaultNS("attr5")

    val attrMap1 = Map(
      attrName1 -> AttributeNumber(1),
      attrName2 -> AttributeNumber(2),
      attrName3 -> AttributeNumber(3)
    )
    val attrMap2 = Map(
      attrName2 -> AttributeNumber(2),
      attrName3 -> AttributeNumber(3),
      attrName4 -> AttributeNumber(4)
    )
    val attrMap3 = Map(
      attrName3 -> AttributeNumber(3),
      attrName4 -> AttributeNumber(4),
      attrName5 -> AttributeNumber(5)
    )

    val thing1 = Entity("one", "thing", attrMap1)
    val thing2 = Entity("two", "thing", attrMap2)
    val thing3 = Entity("three", "thing", attrMap3)

    val item1 = Entity("one", "item", attrMap1)
    val item2 = Entity("two", "item", attrMap2)
    val item3 = Entity("three", "item", attrMap3)

    // insert the things and items to both workspace1 and workspace2
    Seq(thing1, thing2, thing3, item1, item2, item3).foreach { entity =>
      Await.result(
        ws1Provider.createEntity(entity, defaultRequestContext),
        atMost
      )
      Await.result(
        ws2Provider.createEntity(entity, defaultRequestContext),
        atMost
      )
    }

    // verify metadata
    val expectedAttributesBefore = Seq(attrName1, attrName2, attrName3, attrName4, attrName5).map(toDelimitedName)
    Seq(ws1Provider, ws2Provider) foreach { provider =>
      val metadata = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
      metadata.size shouldBe 2 // two entity types: "thing" and "item"
      metadata("thing").count shouldBe 3
      metadata("item").count shouldBe 3
      metadata("thing").attributeNames should contain theSameElementsAs expectedAttributesBefore
      metadata("item").attributeNames should contain theSameElementsAs expectedAttributesBefore
    }

    // delete attr1 and attr5 from "thing" in workspace1
    Await.result(ws1Provider.deleteEntityAttributes("thing", Set(attrName1, attrName5), defaultRequestContext), atMost)

    // in workspace1, "thing" should have attr2, attr3, and attr4 but "item" should have all attributes
    val ws1Metadata = Await.result(ws1Provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    ws1Metadata.size shouldBe 2 // two entity types: "thing" and "item"
    ws1Metadata("thing").count shouldBe 3
    ws1Metadata("item").count shouldBe 3
    ws1Metadata("thing").attributeNames should contain theSameElementsAs Seq(attrName2, attrName3, attrName4).map(
      toDelimitedName
    )
    ws1Metadata("item").attributeNames should contain theSameElementsAs expectedAttributesBefore

    // in workspace1, both "thing" and "item" should have all attributes
    val ws2Metadata = Await.result(ws2Provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    ws2Metadata.size shouldBe 2 // two entity types: "thing" and "item"
    ws2Metadata("thing").count shouldBe 3
    ws2Metadata("item").count shouldBe 3
    ws2Metadata("thing").attributeNames should contain theSameElementsAs expectedAttributesBefore
    ws2Metadata("item").attributeNames should contain theSameElementsAs expectedAttributesBefore

  }

  it should "delete attributes containing references" in withMinimalTestDatabase { _ =>
    // create providers for workspace1 and workspace2
    val repository = new CompactEntityRepository(slickDataSource)
    val ws1Provider =
      new CompactEntityProvider(defaultEntityRequestArguments, repository, "testMetricPrefix")(ec, system)
    val ws2Provider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = minimalTestData.workspace2),
      repository,
      "testMetricPrefix"
    )(ec, system)

    // insert target entities into both workspaces
    Range(1, 4).foreach { idx =>
      Await.result(
        ws1Provider.createEntity(Entity(s"targetName$idx", "targetType", Map()), defaultRequestContext),
        atMost
      )
      Await.result(
        ws2Provider.createEntity(Entity(s"targetName$idx", "targetType", Map()), defaultRequestContext),
        atMost
      )
    }

    // define pointers for each of the target entities
    val targetPointer1 = EntityPointer("targetType", "targetName1")
    val targetPointer2 = EntityPointer("targetType", "targetName2")
    val targetPointer3 = EntityPointer("targetType", "targetName3")

    // insert entities containing references into both workspaces
    val attrName1 = AttributeName.withDefaultNS("attr1")
    val attrName2 = AttributeName.withLibraryNS("attr2")
    val attrName3 = AttributeName.fromDelimitedName("import:attr3")

    val sourceEntity1 = Entity(
      "sourceName1",
      "sourceType",
      Map(
        attrName1 -> AttributeEntityReference("targetType", "targetName1"),
        attrName2 -> AttributeEntityReference("targetType", "targetName2")
      )
    )
    val sourceEntity2 = Entity(
      "sourceName2",
      "sourceType",
      Map(
        attrName2 -> AttributeEntityReference("targetType", "targetName2"),
        attrName3 -> AttributeEntityReference("targetType", "targetName3")
      )
    )

    Seq(sourceEntity1, sourceEntity2).foreach { entity =>
      Await.result(ws1Provider.createEntity(entity, defaultRequestContext), atMost)
      Await.result(ws2Provider.createEntity(entity, defaultRequestContext), atMost)
    }

    // verify references in both workspaces
    Seq(ws1Provider, ws2Provider) foreach { provider =>
      runAndWait(
        provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName1"))
      ) should contain theSameElementsAs Seq(targetPointer1, targetPointer2)
      runAndWait(
        provider.repository.queries.getReferencesFrom(minimalTestData.workspace2.workspaceIdAsUUID,
                                                      EntityPointer("sourceType", "sourceName2")
        )
      ) should contain theSameElementsAs Seq(targetPointer2, targetPointer3)
    }

    // delete attr1 and attr3 from "sourceType" in workspace1 only
    Await.result(
      ws1Provider.deleteEntityAttributes("sourceType", Set(attrName1, attrName3), defaultRequestContext),
      atMost
    )
    // retrieve references for workspace 1 and ensure the references were deleted
    runAndWait(
      ws1Provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName1"))
    ) should contain theSameElementsAs Seq(targetPointer2)
    runAndWait(
      ws1Provider.repository.queries.getReferencesFrom(wsid, EntityPointer("sourceType", "sourceName2"))
    ) should contain theSameElementsAs Seq(targetPointer2)

    // retrieve references for workspace 2 and ensure the references were NOT deleted
    runAndWait(
      ws2Provider.repository.queries.getReferencesFrom(minimalTestData.workspace2.workspaceIdAsUUID,
                                                       EntityPointer("sourceType", "sourceName1")
      )
    ) should contain theSameElementsAs Seq(targetPointer1, targetPointer2)
    runAndWait(
      ws2Provider.repository.queries.getReferencesFrom(minimalTestData.workspace2.workspaceIdAsUUID,
                                                       EntityPointer("sourceType", "sourceName2")
      )
    ) should contain theSameElementsAs Seq(targetPointer2, targetPointer3)
  }

  behavior of "copyEntities"

  it should "copy entities without references from source to destination workspace" in withMinimalTestDatabase { _ =>
    val sourceWorkspace = minimalTestData.workspace2
    val destinationWorkspace = minimalTestData.workspace

    val sourceProvider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = sourceWorkspace),
      new CompactEntityRepository(slickDataSource),
      "testMetricPrefix"
    )(ec, system)

    val destinationProvider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = destinationWorkspace),
      new CompactEntityRepository(slickDataSource),
      "testMetricPrefix"
    )(ec, system)

    // insert entities to be copied to source workspace
    val entityA1 = Entity("name1", "typeA", Map())
    val entityA2 = Entity("name2", "typeA", Map())
    val entityB1 = Entity("name3", "typeB", Map())
    Await.result(sourceProvider.createEntity(entityA1, defaultRequestContext), atMost) shouldBe entityA1
    Await.result(sourceProvider.createEntity(entityA2, defaultRequestContext), atMost) shouldBe entityA2
    Await.result(sourceProvider.createEntity(entityB1, defaultRequestContext), atMost) shouldBe entityB1

    // validate results of creations
    val metadataAfter =
      Await.result(sourceProvider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    // validate the destination workspace is empty before copying

    Await.result(destinationProvider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost) shouldBe empty

    // copy entities from workspace 2 to workspace 1
    val copyResult =
      Await.result(
        sourceProvider.copyEntities(
          sourceWorkspace,
          destinationWorkspace,
          entityA1.entityType,
          Seq(entityA1.name, entityA2.name),
          linkExistingEntities = false,
          defaultRequestContext
        ),
        atMost
      )
    copyResult.hardConflicts shouldBe empty
    copyResult.softConflicts shouldBe empty
    copyResult.entitiesCopied should contain theSameElementsAs Seq(entityA1.toReference, entityA2.toReference)

    // validate results of copying
    val metadataDestination =
      Await.result(destinationProvider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataDestination.size shouldBe 1
    metadataDestination.keys should contain theSameElementsAs Seq("typeA")
    metadataDestination("typeA").count shouldBe 2
  }

  it should "copy entities and entity references from source to destination workspace" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("ref"), AttributeEntityReference("typeA", "name1")))
      ),
      EntityUpdateDefinition(
        "name3",
        "typeB",
        Seq(
          AddUpdateAttribute(
            AttributeName.withDefaultNS("refs"),
            AttributeEntityReferenceList(
              Seq(
                AttributeEntityReference("typeA", "name1"),
                AttributeEntityReference("typeA", "name2")
              )
            )
          )
        )
      )
    )

    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 3

    val copiedEntities = Await.result(
      provider.copyEntities(minimalTestData.workspace,
                            minimalTestData.workspace2,
                            "typeB",
                            Seq("name3"),
                            linkExistingEntities = false,
                            defaultRequestContext
      ),
      atMost
    )
    copiedEntities.entitiesCopied should contain theSameElementsAs Seq(
      AttributeEntityReference("typeA", "name1"),
      AttributeEntityReference("typeA", "name2"),
      AttributeEntityReference("typeB", "name3")
    )
    copiedEntities.hardConflicts shouldBe empty
    copiedEntities.softConflicts shouldBe empty

  }

  it should "not copy entity that does not exist in source workspace" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()
    val copiedEntities = Await.result(
      provider.copyEntities(
        minimalTestData.workspace,
        minimalTestData.workspace2,
        "typeA",
        Seq("nonExistentEntity"),
        linkExistingEntities = false,
        defaultRequestContext
      ),
      atMost
    )
    copiedEntities.entitiesCopied shouldBe empty
    copiedEntities.hardConflicts shouldBe empty
    copiedEntities.softConflicts shouldBe empty
  }

  behavior of "evaluateExpressions"

  List(None, Some("this")) foreach { expressionUnderTest =>
    val entityType = "cat"
    val fooAttribute: AttributeName = AttributeName.withDefaultNS("foo")
    val exemplarDataWithCommonNames: Seq[Entity] =
      Seq(
        Entity(s"001", entityType, Map(fooAttribute -> AttributeString(s"$entityType-001"))),
        Entity(s"002", entityType, Map(fooAttribute -> AttributeString(s"$entityType-002"))),
        Entity(s"003", entityType, Map(fooAttribute -> AttributeString(s"$entityType-003")))
      )

    it should s"allow $expressionUnderTest as a root expression" in withMinimalTestDatabase { _ =>
      // save exemplar data
      runAndWait(
        compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                              exemplarDataWithCommonNames,
                                              insertOnly = false
        )
      )

      // get provider
      val provider = defaultProvider()

      // set up arguments for expression evaluation
      val expressionEvaluationContext =
        ExpressionEvaluationContext(Option(entityType), Option("002"), expressionUnderTest, Option(entityType))

      val toolInputParameter = new ToolInputParameter()
        .name("my-input-name")
        .valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING))
      val processableInputs = Set(MethodInput(toolInputParameter, "this.foo"))
      val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

      val submissionValidationEntityInputsList =
        Await
          .result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()), atMost)
          .toList
      submissionValidationEntityInputsList.size shouldBe 1

      val entityInputs = submissionValidationEntityInputsList.head
      entityInputs.entityName shouldBe "002"
      entityInputs.inputResolutions.size shouldBe 1
      entityInputs.inputResolutions.head.error shouldBe empty
      entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"
      entityInputs.inputResolutions.head.value should contain(AttributeString(s"$entityType-002"))
    }

    it should s"validate entity type even with $expressionUnderTest as a root expression" in withMinimalTestDatabase {
      _ =>
        // save exemplar data
        runAndWait(
          compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                                exemplarDataWithCommonNames,
                                                insertOnly = false
          )
        )

        // get provider
        val provider = defaultProvider()

        // set up arguments for expression evaluation
        // note that entityType and rootEntityType do not match
        val expressionEvaluationContext =
          ExpressionEvaluationContext(Option(s"$entityType-doesnotmatch"),
                                      Option("002"),
                                      expressionUnderTest,
                                      Option(entityType)
          )

        val toolInputParameter = new ToolInputParameter()
          .name("my-input-name")
          .valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING))
        val processableInputs = Set(MethodInput(toolInputParameter, "this.foo"))
        val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

        val actualException =
          intercept[RawlsExceptionWithErrorReport] {
            Await
              .result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()), atMost)
              .toList
          }

        actualException.errorReport.statusCode should contain(StatusCodes.BadRequest)
        actualException.errorReport.message should include("expects an entity of type")
    }

  }

  it should "return set-entity values in the set's order" in withMinimalTestDatabase { _ =>
    // define how many set members this test should use
    val range = Range(0, 100)
    // define that many participants, with an "index" attribute
    val participants = range.map { idx =>
      Entity(s"participant_$idx", "participant", Map(AttributeName.withDefaultNS("index") -> AttributeNumber(idx)))
    }
    // define a participant set containing all participants
    val participantSet = Entity("the-set",
                                "participant_set",
                                Map(
                                  AttributeName.withDefaultNS("participants") -> AttributeEntityReferenceList(
                                    participants.map(_.toReference)
                                  )
                                )
    )

    // save participants
    runAndWait(
      compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                            participants,
                                            insertOnly = false
      )
    )
    // save participant set
    runAndWait(
      compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                            Seq(participantSet),
                                            insertOnly = false
      )
    )

    // get provider
    val provider = defaultProvider()

    // set up arguments for expression evaluation
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Option("participant_set"), Option("the-set"), None, Option("participant_set"))
    val toolInputParameter = new ToolInputParameter()
      .name("my-input-name")
      .valueType(
        new ValueType().typeName(ValueType.TypeNameEnum.ARRAY).arrayType(new ValueType().typeName(TypeNameEnum.INT))
      )
    val processableInputs = Set(MethodInput(toolInputParameter, "this.participants.index"))
    val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

    // evaluate "this.participants.index" against the participant set
    val submissionValidationEntityInputsList =
      Await
        .result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()), atMost)
        .toList
    submissionValidationEntityInputsList.size shouldBe 1

    // basic validation of the expression-evaluation result
    val entityInputs = submissionValidationEntityInputsList.head
    entityInputs.entityName shouldBe "the-set"
    entityInputs.inputResolutions.size shouldBe 1
    entityInputs.inputResolutions.head.error shouldBe empty
    entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"

    val resolvedValue = entityInputs.inputResolutions.head.value
    resolvedValue should not be empty
    resolvedValue.get shouldBe a[AttributeValueList]

    val actual = resolvedValue.get.asInstanceOf[AttributeValueList]
    val expected = AttributeValueList(range.map(idx => AttributeNumber(idx)))

    // did expression-evaluation return the correct attribute values, in _any_ order?
    withClue("evaluated expression should contain the same elements, in any order") {
      actual.list should contain theSameElementsAs expected.list
    }
    // did expression-evaluation return the correct attribute values, in the same order as the set members?
    withClue("evaluated expression should contain the same elements, in the same order") {
      actual.list should contain theSameElementsInOrderAs expected.list
    }
  }

  it should "de-duplicate values if a set contains duplicate references" in withMinimalTestDatabase { _ =>
    // define two participants, with an "index" attribute
    val participants = Seq(
      Entity(s"participant_1", "participant", Map(AttributeName.withDefaultNS("index") -> AttributeNumber(1))),
      Entity(s"participant_2", "participant", Map(AttributeName.withDefaultNS("index") -> AttributeNumber(2))),
      Entity(s"participant_3", "participant", Map(AttributeName.withDefaultNS("index") -> AttributeNumber(3))),
      Entity(s"participant_4", "participant", Map(AttributeName.withDefaultNS("index") -> AttributeNumber(4)))
    )

    // define a participant set containing duplicate references to those participants
    val ref1 = AttributeEntityReference("participant", "participant_1")
    val ref2 = AttributeEntityReference("participant", "participant_2")
    val ref3 = AttributeEntityReference("participant", "participant_3")
    val ref4 = AttributeEntityReference("participant", "participant_4")
    val participantSet = Entity(
      "the-set",
      "participant_set",
      Map(
        AttributeName.withDefaultNS("participants") -> AttributeEntityReferenceList(
          Seq(ref1, ref2, ref1, ref3, ref2, ref4, ref2, ref3, ref1, ref4)
        )
      )
    )

    // save participants
    runAndWait(
      compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                            participants,
                                            insertOnly = false
      )
    )
    // save participant set
    runAndWait(
      compactEntityQuery.batchWriteEntities(minimalTestData.workspace.workspaceIdAsUUID,
                                            Seq(participantSet),
                                            insertOnly = false
      )
    )

    // get provider
    val provider = defaultProvider()

    // set up arguments for expression evaluation
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Option("participant_set"), Option("the-set"), None, Option("participant_set"))
    val toolInputParameter = new ToolInputParameter()
      .name("my-input-name")
      .valueType(
        new ValueType().typeName(ValueType.TypeNameEnum.ARRAY).arrayType(new ValueType().typeName(TypeNameEnum.INT))
      )
    val processableInputs = Set(MethodInput(toolInputParameter, "this.participants.index"))
    val gatherInputsResult = GatherInputsResult(processableInputs, Set(), Set(), Set())

    // evaluate "this.participants.index" against the participant set
    val submissionValidationEntityInputsList =
      Await
        .result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map()), atMost)
        .toList
    submissionValidationEntityInputsList.size shouldBe 1

    // basic validation of the expression-evaluation result
    val entityInputs = submissionValidationEntityInputsList.head
    entityInputs.entityName shouldBe "the-set"
    entityInputs.inputResolutions.size shouldBe 1
    entityInputs.inputResolutions.head.error shouldBe empty
    entityInputs.inputResolutions.head.inputName shouldBe "my-input-name"

    val resolvedValue = entityInputs.inputResolutions.head.value
    resolvedValue should not be empty
    resolvedValue.get shouldBe a[AttributeValueList]

    val actual = resolvedValue.get.asInstanceOf[AttributeValueList]
    val expected = AttributeValueList(
      Seq(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3), AttributeNumber(4))
    )

    // did expression-evaluation return the correct attribute values, in _any_ order?
    withClue("evaluated expression should contain the same elements, in any order") {
      actual.list should contain theSameElementsAs expected.list
    }
    // did expression-evaluation return the correct attribute values, in the same order as the set members?
    withClue("evaluated expression should contain the same elements, in the same order") {
      actual.list should contain theSameElementsInOrderAs expected.list
    }
  }

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository, "testMetricPrefix")(ec, system)
  }

}
