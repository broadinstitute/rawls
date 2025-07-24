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
    val createResult =
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

  it should "invalidate after renameAttribute" is pending
  it should "invalidate after deleteEntityAttributes" is pending

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

  it should "invalidate after deleteEntities" is pending
  it should "invalidate after copyEntities" is pending
  it should "invalidate after saveWorkflowOutputEntities" is pending

  behavior of "clone"

  it should "also clone cache entries" is pending

  behavior of "renameEntityType"

  it should "also rename the cache entry" is pending

  behavior of "deleteEntitiesOfType"

  it should "also delete the cache entry" is pending

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository)(ec, system)
  }

}
