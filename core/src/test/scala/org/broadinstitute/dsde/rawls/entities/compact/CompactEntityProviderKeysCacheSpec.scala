package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.{Sink, Source}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  EntityTypeAndAttributeKeys,
  TestDriverComponentWithFlatSpecAndMatchers
}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException}
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
class CompactEntityProviderKeysCacheSpec extends TestDriverComponentWithFlatSpecAndMatchers {

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

  behavior of "entityTypeMetadata"

  it should "use valid cache entries" is pending

  it should "persist invalid cache entries" is pending

  it should "persist missing cache entries" is pending

  it should "ignore extraneous cache entries" is pending

  behavior of "single entity-type cache invalidation"

  it should "invalidate after createEntity" in withMinimalTestDatabase { _ =>
    // insert valid cache entry
    val cacheEntry = EntityTypeAndAttributeKeys("entityType1", Set(AttributeName.withDefaultNS("attr1")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry))) shouldBe 1
    // validate cache entry
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntry)
    // create entity
    val provider = defaultProvider()
    val entity = Entity("name", cacheEntry.entityType, Map())
    val createResult =
      Await.result(provider.createEntity(entity, defaultRequestContext), atMost)
    // validate entity created
    createResult shouldBe entity
    // validate cache entry invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  it should "invalidate after updateEntity" in withMinimalTestDatabase { _ =>
    val entityType = "entityType1"
    // create entity
    val provider = defaultProvider()
    val entity = Entity("name", entityType, Map())
    Await.result(provider.createEntity(entity, defaultRequestContext), atMost)
    // insert valid cache entry
    val cacheEntry = EntityTypeAndAttributeKeys(entityType, Set(AttributeName.withDefaultNS("attr1")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry))) shouldBe 1
    // validate cache entry
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntry)
    // update entity
    val ops = Seq(AddUpdateAttribute(AttributeName.withDefaultNS("attr1"), AttributeString("value")))
    val updateResult =
      Await.result(provider.updateEntity(entity.entityType, entity.name, ops, defaultRequestContext), atMost)
    updateResult shouldBe entity.copy(attributes =
      Map(AttributeName.withDefaultNS("attr1") -> AttributeString("value"))
    )
    // validate cache entry invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  it should "invalidate after renameAttribute" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // insert the entities to be updated
    val entityA1 = Entity("name1", "typeA", Map(AttributeName.withDefaultNS("old") -> AttributeNumber(42)))
    Await.result(provider.createEntity(entityA1, defaultRequestContext), atMost)

    // insert valid cache entries for typeA
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntryA))) shouldBe 1
    // validate cache entries
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryA)

    // perform the attribute rename
    val numUpdated = Await.result(
      provider.renameAttribute(entityA1.entityType,
                               AttributeName.withDefaultNS("old"),
                               AttributeRename(AttributeName.withDefaultNS("new")),
                               defaultRequestContext
      ),
      atMost
    )
    numUpdated shouldBe 1

    // validate that typeA and typeB cache entries are invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  it should "invalidate after deleteEntityAttributes" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // insert the entities to be updated
    val entityA1 = Entity("name1", "typeA", Map(AttributeName.withDefaultNS("deleteme1") -> AttributeNumber(42)))
    val entityA2 = Entity("name2", "typeA", Map(AttributeName.withDefaultNS("deleteme2") -> AttributeNumber(123)))
    Await.result(provider.createEntity(entityA1, defaultRequestContext), atMost)
    Await.result(provider.createEntity(entityA2, defaultRequestContext), atMost)

    // insert valid cache entries for typeA
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))

    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntryA))) shouldBe 1
    // validate cache entries
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryA)

    // perform the attribute delete
    Await.result(
      provider.deleteEntityAttributes(entityA1.entityType,
                                      Set(AttributeName.withDefaultNS("deleteme1"),
                                          AttributeName.withDefaultNS("deleteme2")
                                      ),
                                      defaultRequestContext
      ),
      atMost
    )

    // validate that typeA and typeB cache entries are invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  it should "invalidate after deleteEntitiesOfType" in withMinimalTestDatabase { _ =>
    val entityType = "entityType1"
    // create entity
    val provider = defaultProvider()
    val entity = Entity("name", entityType, Map())
    Await.result(provider.createEntity(entity, defaultRequestContext), atMost)
    // insert valid cache entry
    val cacheEntry = EntityTypeAndAttributeKeys(entityType, Set(AttributeName.withDefaultNS("attr1")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry))) shouldBe 1
    // validate cache entry
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntry)
    // delete entities of this type
    val deleteResult =
      Await.result(provider.deleteEntitiesOfType(entity.entityType, defaultRequestContext), atMost)
    deleteResult shouldBe 1
    // validate cache entry invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  behavior of "multiple entity-type cache invalidation"

  it should "invalidate after batchUpsertEntities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // define updates for typeA and typeB
    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq()),
      EntityUpdateDefinition("name3", "typeB", Seq())
    )

    // insert valid cache entries for typeA, typeB, and typeC
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))
    val cacheEntryB = EntityTypeAndAttributeKeys("typeB", Set(AttributeName.withDefaultNS("attr2")))
    val cacheEntryC = EntityTypeAndAttributeKeys("typeC", Set(AttributeName.withDefaultNS("attr3")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntryA, cacheEntryB, cacheEntryC))) shouldBe 3
    // validate cache entries
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryA, cacheEntryB, cacheEntryC)

    // perform the batch upsert
    val numUpdated = Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 3
    // validate results of batch upsert
    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    // validate that typeA and typeB cache entries are invalidated
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryC)
  }

  it should "invalidate after batchUpdateEntities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // insert entities to be updated
    val entityA1 = Entity("name1", "typeA", Map())
    val entityA2 = Entity("name2", "typeA", Map())
    val entityB1 = Entity("name3", "typeB", Map())
    Await.result(provider.createEntity(entityA1, defaultRequestContext), atMost)
    Await.result(provider.createEntity(entityA2, defaultRequestContext), atMost)
    Await.result(provider.createEntity(entityB1, defaultRequestContext), atMost)

    // define updates for typeA and typeB
    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("bar")))
      ),
      EntityUpdateDefinition("name2",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("baz")))
      ),
      EntityUpdateDefinition("name3",
                             "typeB",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("foo"), AttributeString("qux")))
      )
    )

    // insert valid cache entries for typeA, typeB, and typeC
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))
    val cacheEntryB = EntityTypeAndAttributeKeys("typeB", Set(AttributeName.withDefaultNS("attr2")))
    val cacheEntryC = EntityTypeAndAttributeKeys("typeC", Set(AttributeName.withDefaultNS("attr3")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntryA, cacheEntryB, cacheEntryC))) shouldBe 3
    // validate cache entries
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryA, cacheEntryB, cacheEntryC)

    // perform the batch upsert
    val numUpdated = Await.result(provider.batchUpdateEntities(Source(updates), defaultRequestContext), atMost)
    numUpdated shouldBe 3
    // validate results of batch upsert
    val metadataAfter = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    // validate that typeA and typeB cache entries are invalidated
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryC)
  }

  it should "invalidate after deleteEntities" in withMinimalTestDatabase { _ =>
    val provider = defaultProvider()

    // insert entities to be deleted
    val entityA1 = Entity("name1", "typeA", Map())
    val entityA2 = Entity("name2", "typeA", Map())
    val entityB1 = Entity("name3", "typeB", Map())
    Await.result(provider.createEntity(entityA1, defaultRequestContext), atMost)
    Await.result(provider.createEntity(entityA2, defaultRequestContext), atMost)
    Await.result(provider.createEntity(entityB1, defaultRequestContext), atMost)

    // insert valid cache entries for typeA, typeB, and typeC
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))
    val cacheEntryB = EntityTypeAndAttributeKeys("typeB", Set(AttributeName.withDefaultNS("attr2")))
    val cacheEntryC = EntityTypeAndAttributeKeys("typeC", Set(AttributeName.withDefaultNS("attr3")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntryA, cacheEntryB, cacheEntryC))) shouldBe 3
    // validate cache entries
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryA, cacheEntryB, cacheEntryC)

    // delete some of the entities from typeA and typeB
    val numDeleted =
      Await.result(provider.deleteEntities(Seq(entityA1.toPointer, entityB1.toPointer), defaultRequestContext), atMost)
    numDeleted shouldBe 2

    // validate that typeA and typeB cache entries are invalidated
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntryC)
  }

  it should "invalidate after copyEntities" ignore withMinimalTestDatabase { _ =>
    val sourceWorkspace = minimalTestData.workspace2
    val destinationWorkspace = minimalTestData.workspace

    val sourceProvider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = sourceWorkspace),
      new CompactEntityRepository(slickDataSource)
    )(ec, system)

    val destinationProvider = new CompactEntityProvider(
      defaultEntityRequestArguments.copy(workspace = destinationWorkspace),
      new CompactEntityRepository(slickDataSource)
    )(ec, system)

    // insert entities to be copied to source workspace
    val entityA1 = Entity("name1", "typeA", Map())
    val entityA2 = Entity("name2", "typeA", Map())
    val entityB1 = Entity("name3", "typeB", Map())
    Await.result(sourceProvider.createEntity(entityA1, defaultRequestContext), atMost) shouldBe entityA1
    Await.result(sourceProvider.createEntity(entityA2, defaultRequestContext), atMost) shouldBe entityA2
    Await.result(sourceProvider.createEntity(entityB1, defaultRequestContext), atMost) shouldBe entityB1

    // validate results of creations
    val metadataAfter = Await.result(sourceProvider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataAfter.size shouldBe 2
    metadataAfter.keys should contain theSameElementsAs Seq("typeA", "typeB")
    metadataAfter("typeA").count shouldBe 2
    metadataAfter("typeB").count shouldBe 1

    // insert valid cache entries for typeA, typeB, and typeC to workspace 1
    val cacheEntryA = EntityTypeAndAttributeKeys("typeA", Set(AttributeName.withDefaultNS("attr1")))
    val cacheEntryB = EntityTypeAndAttributeKeys("typeB", Set(AttributeName.withDefaultNS("attr2")))
    val cacheEntryC = EntityTypeAndAttributeKeys("typeC", Set(AttributeName.withDefaultNS("attr3")))
    // save the cache
    runAndWait(
      q.saveCache(destinationWorkspace.workspaceIdAsUUID, Set(cacheEntryA, cacheEntryB, cacheEntryC))
    ) shouldBe 3
    // validate cache entries
    runAndWait(q.getCachedKeys(destinationWorkspace.workspaceIdAsUUID)) should contain theSameElementsAs Seq(
      cacheEntryA,
      cacheEntryB,
      cacheEntryC
    )

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

    // validate results of copying
    val metadataDestination =
      Await.result(destinationProvider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)
    metadataDestination.size shouldBe 1
    metadataDestination.keys should contain theSameElementsAs Seq("typeA")
    metadataDestination("typeA").count shouldBe 2

    copyResult.hardConflicts shouldBe empty
    copyResult.softConflicts shouldBe empty
    copyResult.entitiesCopied should contain theSameElementsAs Seq(entityA1.toReference, entityA2.toReference)

    // validate that typeA cache entry is invalidated
    runAndWait(q.getCachedKeys(destinationWorkspace.workspaceIdAsUUID)) should contain theSameElementsAs Seq(
      cacheEntryB,
      cacheEntryC
    )
  }

  it should "invalidate after saveWorkflowOutputEntities" in withMinimalTestDatabase { dataSource =>
    val entityType = "entityType1"
    // create entity
    val provider = defaultProvider()
    val entity = Entity("name", entityType, Map())
    Await.result(provider.createEntity(entity, defaultRequestContext), atMost)
    // insert valid cache entry
    val cacheEntry = EntityTypeAndAttributeKeys(entityType, Set(AttributeName.withDefaultNS("attr1")))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry))) shouldBe 1
    // validate cache entry
    runAndWait(q.getCachedKeys(wsid)) should contain theSameElementsAs Seq(cacheEntry)
    // call saveWorkflowOutputEntities on this entity

    val updatedEntity = entity.copy(attributes = Map(AttributeName.withDefaultNS("attr1") -> AttributeString("value")))
    val updateResult =
      runAndWait(
        provider.saveWorkflowOutputEntities(dataSource.dataAccess, minimalTestData.workspace, Seq(updatedEntity))
      )
    // note this returns 2, not 1. When MySQL updates an existing row in
    // an "insert ... on duplicate key update" statement, it counts that as 2 rows affected.
    updateResult shouldBe 2
    // validate cache entry invalidated
    runAndWait(q.getCachedKeys(wsid)) shouldBe empty
  }

  behavior of "clone"

  // this is an optimization; it would be correct but slower without this
  it should "also clone cache entries" is pending

  behavior of "renameEntityType"

  // this is an optimization; it would be correct but slower without this
  it should "also rename the cache entry" is pending

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository)(ec, system)
  }

}
