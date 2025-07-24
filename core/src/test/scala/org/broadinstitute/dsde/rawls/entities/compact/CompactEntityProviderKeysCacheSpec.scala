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

  it should "read valid cache entries" is pending

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

  it should "invalidate after updateEntity" is pending
  it should "invalidate after renameAttribute" is pending
  it should "invalidate after deleteEntityAttributes" is pending

  behavior of "multiple entity-type cache invalidation"

  it should "invalidate after batchUpsertEntities" is pending
  it should "invalidate after batchUpdateEntities" is pending
  it should "invalidate after copyEntities" is pending
  it should "invalidate after saveWorkflowOutputEntities" is pending

  behavior of "clone"

  it should "also clone cache entries" is pending

  behavior of "renameEntityType"

  it should "also rename the cache entry" is pending

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository)(ec, system)
  }

}
