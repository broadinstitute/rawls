package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}

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
  // AttributeSuppport.applyOperationsToEntity, which is tested elsewhere.

  it should "create entities when asked" in withMinimalTestDatabase { _ =>
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

  it should "create entities with internal references" in withMinimalTestDatabase { dataSource =>
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

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  def defaultProvider(): CompactEntityProvider = {
    val repository = new CompactEntityRepository(slickDataSource)
    new CompactEntityProvider(defaultEntityRequestArguments, repository)(ec, system)
  }

}
