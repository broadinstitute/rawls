package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.EntityNotFoundException
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddListMember,
  AddUpdateAttribute,
  CreateAttributeEntityReferenceList,
  EntityUpdateDefinition,
  RemoveAttribute
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeString,
  Entity,
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

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

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

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    // find the ids for the three entities already created
    val entityRecs = runAndWait(
      q.getEntityRefs(wsid,
                      Set(
                        AttributeEntityReference("typeA", "name1"),
                        AttributeEntityReference("typeA", "name2"),
                        AttributeEntityReference("typeB", "name3")
                      )
      )
    )

    val recIdLookup: Map[AttributeEntityReference, Long] = entityRecs.map { rec =>
      rec.toAttributeEntityReference -> rec.id
    }.toMap

    val name1Id = recIdLookup(AttributeEntityReference("typeA", "name1"))
    val name2Id = recIdLookup(AttributeEntityReference("typeA", "name2"))
    val name3Id = recIdLookup(AttributeEntityReference("typeB", "name3"))

    // entity with "name1" should have no references
    runAndWait(q.getReferencedIds(name1Id)) shouldBe empty

    // entity with "name2" should have a reference to "name1"
    runAndWait(q.getReferencedIds(name2Id)) shouldBe Seq(name1Id)

    // entity with "name3" should have references to both "name1" and "name2"
    runAndWait(q.getReferencedIds(name3Id)) should contain theSameElementsAs Seq(
      name1Id,
      name2Id
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
      runAndWait(provider.repository.queries.getReferenceTargets(wsid, "sourceType", "sourceName"))
        .map(_.toAttributeEntityReference)

    initialReferences shouldBe Seq(AttributeEntityReference("targetType", "targetName1"),
                                   AttributeEntityReference("targetType", "targetName2")
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

    Await.result(provider.batchUpsertEntities(updates, defaultRequestContext), atMost)

    // validate the references after our batchUpsert
    val finalReferences =
      runAndWait(provider.repository.queries.getReferenceTargets(wsid, "sourceType", "sourceName"))
        .map(_.toAttributeEntityReference)

    finalReferences.toSet shouldBe Set(
      AttributeEntityReference("targetType", "targetName1"),
      AttributeEntityReference("targetType", "targetName3"),
      AttributeEntityReference("targetType", "targetName4")
    )

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
      runAndWait(provider.repository.queries.getReferenceTargets(wsid, "sourceType", "sourceName"))
        .map(_.toAttributeEntityReference)

    initialReferences shouldBe Seq(AttributeEntityReference("targetType", "targetName"))

    // perform the batchUpsert - delete the existing reference, don't add any
    val updates = Source(
      Seq(
        EntityUpdateDefinition("sourceName", "sourceType", Seq(RemoveAttribute(AttributeName.withDefaultNS("ref"))))
      )
    )

    Await.result(provider.batchUpsertEntities(updates, defaultRequestContext), atMost)

    // validate the references after our batchUpsert
    val finalReferences =
      runAndWait(provider.repository.queries.getReferenceTargets(wsid, "sourceType", "sourceName"))
        .map(_.toAttributeEntityReference)

    finalReferences.toSet shouldBe empty

  }

  it should "create entities across multiple batches" in withMinimalTestDatabase { _ =>
    val repository = new CompactEntityRepository(slickDataSource)
    val config = CompactEntityProviderConfig(batchUpsertBatchSize = 250) // pretty small to force batching

    val provider = new CompactEntityProvider(defaultEntityRequestArguments, repository, config)(ec, system)

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

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 1
    metadataAfter.keys should contain theSameElementsAs Seq("typeA")
    metadataAfter("typeA").count shouldBe 1000
  }

  it should "roll back all writes, even across batches, on error" in withMinimalTestDatabase { _ =>
    val repository = new CompactEntityRepository(slickDataSource)
    val config = CompactEntityProviderConfig(batchUpsertBatchSize = 1) // should execute one update per batch

    val provider = new CompactEntityProvider(defaultEntityRequestArguments, repository, config)(ec, system)

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
    intercept[SQLException] {
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

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

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

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository)(ec, system)
  }

}
