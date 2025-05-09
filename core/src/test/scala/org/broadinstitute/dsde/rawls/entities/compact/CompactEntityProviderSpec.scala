package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityNotFoundException, EntityReferenceNotFoundException}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.entities.compact.entityQuery.CountAndSource
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException,
  EntityReferenceNotFoundException
}
import org.broadinstitute.dsde.rawls.entities.compact.entityQuery.CountAndSource

import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  Entity,
  EntityQuery,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SortDirections,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString, eq => mockitoEq}
import org.mockito.Mockito
import org.mockito.Mockito.{never, timeout => mockitotimeout, times, verify, when}
import org.scalatest.concurrent.Futures.{scaled, PatienceConfig}
import org.scalatest.time.{Millis, Seconds, Span}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CompactEntityProviderSpec extends TestDriverComponentWithFlatSpecAndMatchers with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val system: ActorSystem = ActorSystem("CompactEntityProviderSpec")

  // private val slickDataSource = DbResource.dataSource // needed for transactions in the provider
  private val atMost = Duration("60 seconds") // timeout for Await() in tests
  implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(200, Millis))) // timeout for eventually()

  private val defaultRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  private val defaultWorkspace = Workspace(
    "entityManagerSpecTestWorkspace",
    "entityManagerSpecTestNamespace",
    UUID.randomUUID.toString,
    "bucketName",
    Some("workflowCollection"),
    new DateTime(),
    new DateTime(),
    "creator",
    Map.empty
  )

  private val defaultEntityRequestArguments =
    EntityRequestArguments(defaultWorkspace, defaultRequestContext)

  // extender for

  // ====================================================================================================
  // tests for CompactEntityProvider public implementations of EntityProvider methods
  // ====================================================================================================

  "batchUpdateEntities" should "have tests" is pending

  behavior of "batchUpsertEntities"

  it should "issue one insert statement for multiple entities" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.batchCreateEntities(any(), any())).thenReturn(DBIO.successful(0))
    when(mockQuery.getEntityRefs(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.upsertReferences(any())).thenReturn(DBIO.successful(-1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq()),
      EntityUpdateDefinition("name3", "typeB", Seq())
    )

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    // should have called one batch-insert to write the entities
    verify(mockQuery, times(1)).batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any())
    // entities have no references, so the input to upsertReferences should be empty
    verify(mockQuery, times(1)).upsertReferences(Set())
  }

  it should "issue multiple insert statements when given large batches" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.batchCreateEntities(any(), any())).thenReturn(DBIO.successful(0))
    when(mockQuery.getEntityRefs(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.upsertReferences(any())).thenReturn(DBIO.successful(-1))

    val config = CompactEntityProviderConfig(maxSqlBatchSizeBytes = 2048) // pretty small to force batching

    // provider using mocks
    val provider = providerWithMocks(mockQuery, config = config)

    // define 100 entities, each with a text attribute ranging from 8 to 24 bytes
    val updates: Seq[EntityUpdateDefinition] = Range(0, 100) map { idx =>
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

    // should have called batchCreateEntities multiple times to write the entities
    verify(mockQuery, Mockito.atLeast(2)).batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any())
    // entities have no references, so the input to upsertReferences should be empty
    verify(mockQuery, Mockito.atLeast(2)).upsertReferences(Set())

  }

  it should "ask to insert references" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.batchCreateEntities(any(), any())).thenReturn(DBIO.successful(-1))
    // this response must be exactly what is expected from the input to batchUpsertEntities
    when(mockQuery.getEntityRefs(any(), any())).thenReturn(
      DBIO.successful(
        Seq(
          CompactEntityRefRecord(2, "name2", "typeA"),
          CompactEntityRefRecord(3, "name3", "typeB"),
          CompactEntityRefRecord(4, "targetName", "targetType")
        )
      )
    )
    when(mockQuery.upsertReferences(any())).thenReturn(DBIO.successful(-1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("ref"), AttributeEntityReference("targetType", "targetName"))
        )
      ),
      EntityUpdateDefinition(
        "name3",
        "typeB",
        Seq(
          AddUpdateAttribute(
            AttributeName.withDefaultNS("refs"),
            AttributeEntityReferenceList(
              Seq(
                AttributeEntityReference("targetType", "targetName")
              )
            )
          )
        )
      )
    )

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    // should have called one batch-insert to write the entities
    verify(mockQuery, times(1)).batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any())
    // entities found references, so should ask to upsert those.
    // given the mock response defined above, we expect references from 2->4 and 3->4
    verify(mockQuery, times(1)).upsertReferences(Set(RefPointers(2, Set(4)), RefPointers(3, Set(4))))
  }

  "copyEntities" should "have tests" is pending

  behavior of "createEntity"

  it should "persist an entity with no attributes" in {
    val entityToCreate = Entity("name", "type", Map())
    val createdEntityRec =
      CompactEntityRecord(42,
                          entityToCreate.name,
                          entityToCreate.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("{}")
      )

    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencedIds(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).getReferencedIds(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, never()).upsertReferences(any())
  }

  it should "persist an entity with simple attributes" in {
    val entityToCreate = Entity("name",
                                "type",
                                Map(
                                  AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                                  AttributeName.withDefaultNS("baz") -> AttributeNumber(123)
                                )
    )
    val createdEntityRec =
      CompactEntityRecord(42,
                          entityToCreate.name,
                          entityToCreate.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":123,"foo":"bar"}""")
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencedIds(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).getReferencedIds(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, never()).upsertReferences(any())
  }

  it should "persist an entity with references" in {
    val entityToCreate = Entity(
      "name",
      "type",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference("referencedType", "referencedName0"),
        AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("referencedType", "referencedName1"),
            AttributeEntityReference("referencedType", "referencedName2")
          )
        )
      )
    )

    val createdEntityRec =
      CompactEntityRecord(
        42,
        entityToCreate.name,
        entityToCreate.entityType,
        defaultWorkspace.workspaceIdAsUUID,
        1,
        deleted = false,
        Some(
          """{"foo":"bar","ref":{"entityName":"referencedName0","entityType":"referencedType"},"refs":[{"entityName":"referencedName1","entityType":"referencedType"},{"entityName":"referencedName2","entityType":"referencedType"}]}"""
        )
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencedIds(any(), any()))
      .thenReturn(DBIO.successful(Seq(0, 1, 2))) // reference lookup returns ids
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved
    when(mockQuery.upsertReferences(any()))
      .thenReturn(DBIO.successful(2))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).getReferencedIds(
      defaultWorkspace.workspaceIdAsUUID,
      Set(
        AttributeEntityReference("referencedType", "referencedName0"),
        AttributeEntityReference("referencedType", "referencedName1"),
        AttributeEntityReference("referencedType", "referencedName2")
      )
    )
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, times(1)).upsertReferences(
      Set(RefPointers(42, Set(0, 1, 2)))
    ) // 42 is the entity id from createdEntityRec; Set(0, 1, 2) are the ids returned from getReferencedIds
  }

  it should "throw EntityReferenceNotFoundException if this entity's references are missing" in {
    // entity to create specifies three references
    val entityToCreate = Entity(
      "name",
      "type",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference("referencedType", "referencedName0"),
        AttributeName.withDefaultNS("refs") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("referencedType", "referencedName1"),
            AttributeEntityReference("referencedType", "referencedName2")
          )
        )
      )
    )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
    when(mockQuery.getReferencedIds(any(), any()))
      .thenReturn(DBIO.successful(Seq(0, 1))) // reference lookup returns only two of the three desired references

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = intercept[EntityReferenceNotFoundException] {
      Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)
    }

    actual shouldBe a[EntityReferenceNotFoundException]

    verify(mockQuery, times(1)).getReferencedIds(
      defaultWorkspace.workspaceIdAsUUID,
      Set(
        AttributeEntityReference("referencedType", "referencedName0"),
        AttributeEntityReference("referencedType", "referencedName1"),
        AttributeEntityReference("referencedType", "referencedName2")
      )
    )
    verify(mockQuery, never()).createEntity(any(), any())
    verify(mockQuery, times(1)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, never()).upsertReferences(any())
  }

  it should "throw RawlsExceptionWithErrorReport if this type & name already exists" in {
    val entityToCreate = Entity("name", "type", Map())
    val createdEntityRec =
      CompactEntityRecord(42,
                          entityToCreate.name,
                          entityToCreate.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("{}")
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(Some(createdEntityRec)))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)
    }

    actual shouldBe a[RawlsExceptionWithErrorReport]

    verify(mockQuery, times(1)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )

    verify(mockQuery, never()).getReferencedIds(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, never()).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, never()).upsertReferences(any())
  }

  private val illegalEntities = Map(
    "illegal entity name" -> Entity("no! @@bad@@", "type", Map()),
    "illegal entity type" -> Entity("name", "no! @@bad@@", Map()),
    "illegal attribute name" -> Entity("name",
                                       "type",
                                       Map(
                                         AttributeName.withDefaultNS("no! @@bad@@") -> AttributeNumber(42)
                                       )
    )
  )

  illegalEntities foreach { case (hint, entityToCreate) =>
    it should s"throw RawlsExceptionWithErrorReport when presented with a(n) $hint" in {
      // provider using mocks
      val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
      val provider = providerWithMocks(mockQuery)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)
      }

      actual shouldBe a[RawlsExceptionWithErrorReport]

      verify(mockQuery, never()).getEntity(any(), any(), any())
      verify(mockQuery, never()).getReferencedIds(any(), any())
      verify(mockQuery, never()).createEntity(any(), any())
      verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
      verify(mockQuery, never()).upsertReferences(any())
    }
  }

  behavior of "deleteEntities"

  it should "remove entities" in {
    val entity1 = Entity("name1",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(123)
                         )
    )
    val createdEntityRec1 =
      CompactEntityRecord(42,
                          entity1.name,
                          entity1.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":123,"foo":"bar"}""")
      )

    val entity2 = Entity("name2",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("boo"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(456)
                         )
    )
    val createdEntityRec2 =
      CompactEntityRecord(41,
                          entity2.name,
                          entity2.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":456,"foo":"boo"}""")
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencesTo(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.deleteAllReferencesFrom(any(), any())).thenReturn(DBIO.successful(1))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec1.entityType),
                          ArgumentMatchers.eq(createdEntityRec1.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec1)))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec2.entityType),
                          ArgumentMatchers.eq(createdEntityRec2.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec2)))
    when(mockQuery.batchHide(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    Await.result(provider.deleteEntities(Seq(entity1.toReference, entity2.toReference), defaultRequestContext), atMost)

    verify(mockQuery, times(1)).getReferencesTo(defaultWorkspace.workspaceIdAsUUID,
                                                Seq(entity1.toReference, entity2.toReference)
    )
    verify(mockQuery, times(1)).deleteAllReferencesFrom(defaultWorkspace.workspaceIdAsUUID,
                                                        Set(entity1.toReference, entity2.toReference)
    )
    verify(mockQuery, times(1)).batchHide(defaultWorkspace.workspaceIdAsUUID,
                                          Seq(entity1.toReference, entity2.toReference)
    )
  }

  it should "throw error if entities are referenced" in {
    val entity1 = Entity("name1",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(123)
                         )
    )
    val createdEntityRec1 =
      CompactEntityRecord(42,
                          entity1.name,
                          entity1.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":123,"foo":"bar"}""")
      )

    val entity2 = Entity("name2",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("boo"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(456)
                         )
    )
    val createdEntityRec2 =
      CompactEntityRecord(41,
                          entity2.name,
                          entity2.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":456,"foo":"boo"}""")
      )

    val referencingEntity =
      Entity("ref",
             "refType",
             Map(
               AttributeName.withDefaultNS("ref") -> AttributeEntityReference(entity1.entityType, entity1.name)
             )
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencesTo(any(), any())).thenReturn(DBIO.successful(Seq(referencingEntity.toReference)))
    when(mockQuery.deleteAllReferencesFrom(any(), any())).thenReturn(DBIO.successful(1))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec1.entityType),
                          ArgumentMatchers.eq(createdEntityRec1.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec1)))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec2.entityType),
                          ArgumentMatchers.eq(createdEntityRec2.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec2)))
    when(mockQuery.batchHide(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val result = intercept[DeleteEntitiesConflictException] {
      Await.result(provider.deleteEntities(Seq(entity1.toReference, entity2.toReference), defaultRequestContext),
                   atMost
      )
    }
    result shouldBe a[DeleteEntitiesConflictException]

    verify(mockQuery, never()).deleteAllReferencesFrom(defaultWorkspace.workspaceIdAsUUID,
                                                       Set(entity1.toReference, entity2.toReference)
    )
    verify(mockQuery, never()).batchHide(defaultWorkspace.workspaceIdAsUUID,
                                         Seq(entity1.toReference, entity2.toReference)
    )
  }

  it should "succeed if referencing entities are among those being deleted" in {
    val entity1 = Entity("name1",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(123)
                         )
    )
    val createdEntityRec1 =
      CompactEntityRecord(42,
                          entity1.name,
                          entity1.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":123,"foo":"bar"}""")
      )

    val entity2 = Entity("name2",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("boo"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(456)
                         )
    )
    val createdEntityRec2 =
      CompactEntityRecord(41,
                          entity2.name,
                          entity2.entityType,
                          defaultWorkspace.workspaceIdAsUUID,
                          1,
                          deleted = false,
                          Some("""{"baz":456,"foo":"boo"}""")
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencesTo(any(), any())).thenReturn(DBIO.successful(Seq(entity2.toReference)))
    when(mockQuery.deleteAllReferencesFrom(any(), any())).thenReturn(DBIO.successful(1))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec1.entityType),
                          ArgumentMatchers.eq(createdEntityRec1.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec1)))
    when(
      mockQuery.getEntity(any[UUID],
                          ArgumentMatchers.eq(createdEntityRec2.entityType),
                          ArgumentMatchers.eq(createdEntityRec2.name)
      )
    )
      .thenReturn(DBIO.successful(Some(createdEntityRec2)))
    when(mockQuery.batchHide(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    Await.result(provider.deleteEntities(Seq(entity1.toReference, entity2.toReference), defaultRequestContext), atMost)

    verify(mockQuery, times(1)).getReferencesTo(defaultWorkspace.workspaceIdAsUUID,
                                                Seq(entity1.toReference, entity2.toReference)
    )
    verify(mockQuery, times(1)).deleteAllReferencesFrom(defaultWorkspace.workspaceIdAsUUID,
                                                        Set(entity1.toReference, entity2.toReference)
    )
    verify(mockQuery, times(1)).batchHide(defaultWorkspace.workspaceIdAsUUID,
                                          Seq(entity1.toReference, entity2.toReference)
    )
  }

  behavior of "deleteEntitiesOfType"

  it should "delete all entities of type" in {
    val entityType = "type"

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencesToType(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.deleteAllReferencesFromType(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.batchHideType(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    Await.result(provider.deleteEntitiesOfType(entityType, defaultRequestContext), atMost)

    verify(mockQuery, times(1)).getReferencesToType(defaultWorkspace.workspaceIdAsUUID, entityType)
    verify(mockQuery, times(1)).deleteAllReferencesFromType(defaultWorkspace.workspaceIdAsUUID, entityType)
    verify(mockQuery, times(1)).batchHideType(defaultWorkspace.workspaceIdAsUUID, entityType)
  }

  it should "throw an error if any of the entities are referenced" in {
    val entity1 = Entity("name1",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(123)
                         )
    )

    val referencingEntity =
      Entity("ref",
             "refType",
             Map(
               AttributeName.withDefaultNS("ref") -> AttributeEntityReference(entity1.entityType, entity1.name)
             )
      )

    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getReferencesToType(any(), any())).thenReturn(DBIO.successful(Seq(referencingEntity.toReference)))
    when(mockQuery.deleteAllReferencesFromType(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.batchHideType(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val result = intercept[DeleteEntitiesOfTypeConflictException] {
      Await.result(provider.deleteEntitiesOfType("type", defaultRequestContext), atMost)
    }

    result shouldBe a[DeleteEntitiesOfTypeConflictException]

    verify(mockQuery, times(1)).getReferencesToType(defaultWorkspace.workspaceIdAsUUID, entity1.entityType)
    verify(mockQuery, never()).deleteAllReferencesFromType(defaultWorkspace.workspaceIdAsUUID, entity1.entityType)
    verify(mockQuery, never()).batchHideType(defaultWorkspace.workspaceIdAsUUID, entity1.entityType)
  }

  "deleteEntityAttributes" should "have tests" is pending

  behavior of "entityTypeMetadata"

  it should "return empty map when no entities exist" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.listEntityKeys(any[UUID]))
      .thenReturn(DBIO.successful(Seq.empty))
    when(mockQuery.countEntitiesGroupedByType(any[UUID]))
      .thenReturn(DBIO.successful(Seq.empty))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)

    actual shouldBe Map()
  }

  it should "return map with entity type and count when entities exist" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    // type1 and type2 have keys, type3 has no keys
    when(mockQuery.listEntityKeys(any[UUID]))
      .thenReturn(
        DBIO.successful(
          Seq(
            EntityTypeAndAttributeKey("type1", AttributeName.withDefaultNS("keyA")),
            EntityTypeAndAttributeKey("type2", AttributeName.withDefaultNS("keyB")),
            EntityTypeAndAttributeKey("type2", AttributeName.withDefaultNS("keyC"))
          )
        )
      )
    when(mockQuery.countEntitiesGroupedByType(any[UUID]))
      .thenReturn(
        DBIO.successful(
          Seq(EntityTypeAndCount("type1", 1), EntityTypeAndCount("type2", 2), EntityTypeAndCount("type3", 3))
        )
      )

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.entityTypeMetadata(useCache = false, defaultRequestContext), atMost)

    actual shouldBe Map(
      "type1" -> EntityTypeMetadata(1, "type1" + Attributable.entityIdAttributeSuffix, Seq("keyA")),
      "type2" -> EntityTypeMetadata(2, "type2" + Attributable.entityIdAttributeSuffix, Seq("keyB", "keyC")),
      "type3" -> EntityTypeMetadata(3, "type3" + Attributable.entityIdAttributeSuffix, Seq())
    )
  }

  "evaluateExpression" should "have tests" is pending
  "evaluateExpressions" should "have tests" is pending
  "expressionValidator" should "have tests" is pending

  behavior of "getEntity"

  it should "return Entity when row exists in db" in {
    val rec: CompactEntityRecord =
      CompactEntityRecord(1, "name", "type", UUID.randomUUID(), -1, deleted = false, Some("{}"))

    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(Some(rec)))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.getEntity("nonexistent-type", "nonexistent-name", defaultRequestContext), atMost)
    val expected = Entity(rec.name, rec.entityType, Map())

    actual shouldBe expected
  }

  it should "return Entity with its attributes" in {
    val attrString = """{"foo":"bar", "baz": 42}"""
    val rec: CompactEntityRecord =
      CompactEntityRecord(1, "name", "type", UUID.randomUUID(), -1, deleted = false, Some(attrString))

    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(Some(rec)))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.getEntity("nonexistent-type", "nonexistent-name", defaultRequestContext), atMost)

    val expected = Entity(rec.name,
                          rec.entityType,
                          Map(
                            AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                            AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                          )
    )

    actual shouldBe expected
  }

  it should "throw EntityNotFoundException if row not found in db" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None))
    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = intercept[EntityNotFoundException] {
      Await.result(provider.getEntity("nonexistent-type", "nonexistent-name", defaultRequestContext), atMost)
    }
    actual shouldBe a[EntityNotFoundException]
  }

  "listEntities" should "have tests" is pending
  "queryEntities" should "have tests" is pending
  "queryEntitiesSource" should "have tests" is pending
  "renameAttribute" should "have tests" is pending
  "renameEntity" should "have tests" is pending
  "renameEntityType" should "have tests" is pending
  "updateEntity" should "have tests" is pending

  // ====================================================================================================
  // tests for CompactEntityProvider helper methods
  // ====================================================================================================

  behavior of "findAllReferences(Entity)"

  it should "return nothing if entity has no references" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
    )
    val entity = Entity("name", "type", attributes)
    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeEntityReference, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

    actual shouldBe empty
  }

  it should "find references in the entity's attributes" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42),
      AttributeName.withDefaultNS("ref") -> AttributeEntityReference("refTypeA", "refName1"),
      AttributeName.withDefaultNS("reflist") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("refTypeB", "refName2"),
          AttributeEntityReference("refTypeB", "refName3")
        )
      )
    )
    val entity = Entity("name", "type", attributes)
    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeEntityReference, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

    val expected = Map(
      AttributeEntityReference("type", "name") ->
        Seq(
          AttributeEntityReference("refTypeA", "refName1"),
          AttributeEntityReference("refTypeB", "refName2"),
          AttributeEntityReference("refTypeB", "refName3")
        )
    )

    actual shouldBe expected
  }

  behavior of "findAllReferences(Seq[Entity])"

  it should "return an empty Map when fed an empty Seq" in {
    val input = Seq()

    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeEntityReference, Seq[AttributeEntityReference]] = provider.findAllReferences(input)

    actual shouldBe empty
  }

  it should "return nothing if entities have no references" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
    )
    val entity1 = Entity("name1", "type", attributes)
    val entity2 = Entity("name2", "type", attributes)
    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeEntityReference, Seq[AttributeEntityReference]] =
      provider.findAllReferences(Seq(entity1, entity2))

    actual shouldBe empty
  }

  it should "find references in the entity's attributes" in {
    // entity1 has multiple references
    val entity1 = Entity(
      "name1",
      "type",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("baz") -> AttributeNumber(42),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference("refTypeA", "refName1"),
        AttributeName.withDefaultNS("reflist") -> AttributeEntityReferenceList(
          Seq(
            AttributeEntityReference("refTypeB", "refName2"),
            AttributeEntityReference("refTypeB", "refName3")
          )
        )
      )
    )
    // entity2 has no references
    val entity2 = Entity("name1",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                         )
    )
    // entity3 has a single reference
    val entity3 = Entity(
      "name1",
      "type",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("baz") -> AttributeNumber(42),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference("refTypeC", "refName4")
      )
    )

    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeEntityReference, Seq[AttributeEntityReference]] =
      provider.findAllReferences(Seq(entity1, entity2, entity3))

    val expected = Map(
      entity1.toReference -> Seq(
        AttributeEntityReference("refTypeA", "refName1"),
        AttributeEntityReference("refTypeB", "refName2"),
        AttributeEntityReference("refTypeB", "refName3")
      ),
      entity3.toReference -> Seq(AttributeEntityReference("refTypeC", "refName4"))
    )

    actual shouldBe expected
  }

  behavior of "replaceReferences"

  it should "skip deletes and upserts when isInsert=true and references are empty" in {
    // provider using mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(), isInsert = true), atMost)

    actual shouldBe (0, 0)

    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, never()).upsertReferences(any())
  }

  it should "skip deletes when isInsert=true and non-empty references" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.upsertReferences(any()))
      .thenAnswer { invocation =>
        DBIO.successful(invocation.getArgument[Set[RefPointers]](0).head.toIds.size)
      }
    when(mockQuery.deleteReferencesWithFilter(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(7, 8, 9), isInsert = true), atMost)

    actual shouldBe (0, 3)

    verify(mockQuery, never()).deleteReferencesWithFilter(any(), any())
    verify(mockQuery, times(1)).upsertReferences(Set(RefPointers(2, Set(7, 8, 9))))
  }

  it should "skip upserts when isInsert=false and references are empty" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.deleteReferencesWithFilter(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))
    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(), isInsert = false), atMost)

    actual shouldBe (Int.MinValue, 0)

    verify(mockQuery, times(1)).deleteReferencesWithFilter(2, Set())
    verify(mockQuery, never()).upsertReferences(any())
  }

  it should "both delete and upsert when isInsert=false and non-empty references" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.upsertReferences(any()))
      .thenAnswer { invocation =>
        DBIO.successful(invocation.getArgument[Set[RefPointers]](0).head.toIds.size)
      }
    when(mockQuery.deleteReferencesWithFilter(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(7, 8, 9), isInsert = false), atMost)

    actual shouldBe (Int.MinValue, 3)

    verify(mockQuery, times(1)).deleteReferencesWithFilter(2, Set(7, 8, 9))
    verify(mockQuery, times(1)).upsertReferences(Set(RefPointers(2, Set(7, 8, 9))))
  }

  behavior of "withWorkspaceLastModified"

  it should "trigger a last-modified update on success of the original future" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))
    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)
    val original = Future.successful(123)
    // we have not yet called updateLastModified
    verify(mockRepository, never()).updateLastModified(defaultWorkspace.workspaceIdAsUUID)
    provider.withWorkspaceLastModified(original)
    // execute and verify original function
    val actual = Await.result(original, atMost)
    actual shouldBe 123
    // we should now call updateLastModified within 1 second
    verify(mockRepository, mockitotimeout(1000).times(1)).updateLastModified(defaultWorkspace.workspaceIdAsUUID)
  }

  it should "not trigger a last-modified update on failure of the original future" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))
    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    // original function throws an error
    val original =
      Future.failed(new RuntimeException("withWorkspaceLastModified intentional failure in original function"))
    provider.withWorkspaceLastModified(original)
    // execute and verify original function, which fails
    val actual = intercept[RuntimeException] {
      Await.result(original, atMost)
    }
    actual shouldBe a[RuntimeException]
    // we want to test that the callback is NOT invoked. Since the callback happens asynchronously, we can't wait for it
    // to NOT happen; we just add a sleep and have to be satisfied the callback isn't invoked in that time.
    Thread.sleep(1000)
    verify(mockRepository, never).updateLastModified(defaultWorkspace.workspaceIdAsUUID)
  }

  it should "not fail if the last-modified update fails" in {
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(
        DBIO.failed(new RuntimeException("withWorkspaceLastModified intentional failure in updateLastModified"))
      )
    // provider using mocks
    val provider = providerWithMocks(mockRepository, requestArguments = defaultEntityRequestArguments)

    val original = Future.successful(123)
    // we have not yet called updateLastModified
    verify(mockRepository, never()).updateLastModified(defaultWorkspace.workspaceIdAsUUID)
    provider.withWorkspaceLastModified(original)
    // execute and verify original function, which should return success even if updateLastModified errors
    val actual = Await.result(original, atMost)
    actual shouldBe 123
    // we should now call updateLastModified within 1 second. This threw an error but it will be ignored.
    verify(mockRepository, mockitotimeout(1000).times(1)).updateLastModified(defaultWorkspace.workspaceIdAsUUID)
  }

  behavior of "prepareQueryEntitiesResult"

  it should "return the correct page count" in {
    val mockRepository = mock[CompactEntityRepository]

    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    val query = EntityQuery(2, 10, "foo", SortDirections.Ascending, None)

    val unfilteredCount = 33
    val filteredCount = 25
    val actual =
      provider.prepareQueryEntitiesResult(query, unfilteredCount, CountAndSource(filteredCount, Source.empty))

    actual._1 shouldBe EntityQueryResultMetadata(unfilteredCount, filteredCount, 3)
  }

  it should "fail if the requested page is too big" in {
    val mockRepository = mock[CompactEntityRepository]

    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    val query = EntityQuery(20, 10, "foo", SortDirections.Ascending, None)

    val unfilteredCount = 33
    val filteredCount = 25

    val error = intercept[DataEntityException] {
      provider.prepareQueryEntitiesResult(query, unfilteredCount, CountAndSource(filteredCount, Source.empty))
    }
    error.code shouldBe StatusCodes.BadRequest
  }

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  private def providerWithMocks(queries: CompactEntityQuery,
                                requestArguments: EntityRequestArguments = defaultEntityRequestArguments,
                                config: CompactEntityProviderConfig = CompactEntityProviderConfig()
  ): CompactEntityProvider = {
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries)
      .thenReturn(queries)
    when(mockRepository.dataSource).thenReturn(slickDataSource)

    providerWithMocks(mockRepository, requestArguments, config)
  }

  private def providerWithMocks(repository: CompactEntityRepository,
                                requestArguments: EntityRequestArguments,
                                config: CompactEntityProviderConfig
  ): CompactEntityProvider =
    new CompactEntityProvider(requestArguments, repository, config)(ec, system)

  private def providerWithMocks(repository: CompactEntityRepository,
                                requestArguments: EntityRequestArguments
  ): CompactEntityProvider =
    new CompactEntityProvider(requestArguments, repository)(ec, system)

}
