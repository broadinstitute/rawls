package org.broadinstitute.dsde.rawls.entities.compact

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RefMapping, _}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.compact.entityQuery.CountAndSource
import org.broadinstitute.dsde.rawls.entities.exceptions._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddUpdateAttribute,
  AttributeUpdateOperation,
  EntityUpdateDefinition
}
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeRename,
  AttributeString,
  Entity,
  EntityHardConflict,
  EntityPointer,
  EntityQuery,
  EntityQueryResultMetadata,
  EntitySoftConflict,
  EntityTypeMetadata,
  EntityTypeRename,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SortDirections,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, MockitoTestUtils}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString, eq => mockitoEq}
import org.mockito.Mockito.{never, timeout => mockitotimeout, times, verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.Futures.{scaled, PatienceConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CompactEntityProviderSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with MockitoTestUtils
    with AttributeSupport {

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

  behavior of "batchUpsertEntities and batchUpdateEntities"

  it should "issue one insert statement for multiple entities" in {
    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.batchCreateEntities(any(), any(), any())).thenReturn(DBIO.successful(0))
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.getEntities(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.getEntityVersions(any(), any())).thenReturn(DBIO.successful(Seq()))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition("name2", "typeA", Seq()),
      EntityUpdateDefinition("name3", "typeB", Seq())
    )

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    // should have called one batch-insert to write the entities
    verify(mockQuery, times(1)).batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any(), any())
  }

  it should "issue multiple insert statements when given large batches" in {
    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.batchCreateEntities(any(), any(), any())).thenReturn(DBIO.successful(0))
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.getEntities(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.getEntityVersions(any(), any())).thenReturn(DBIO.successful(Seq()))

    val config = CompactEntityProviderConfig(batchUpsertBatchSize = 25) // pretty small to force batching

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
    verify(mockQuery, Mockito.atLeast(2))
      .batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any(), any())
  }

  it should "ask to insert references" in {
    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.batchCreateEntities(any(), any(), any())).thenReturn(DBIO.successful(-1))
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.getEntities(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.getEntityVersions(any(), any())).thenReturn(DBIO.successful(Seq()))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val refTarget = EntityPointer("targetType", "targetName")

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1", "typeA", Seq()),
      EntityUpdateDefinition(
        "name2",
        "typeA",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("ref"), refTarget.toAttributeEntityReference)
        )
      ),
      EntityUpdateDefinition(
        "name3",
        "typeB",
        Seq(
          AddUpdateAttribute(
            AttributeName.withDefaultNS("refs"),
            AttributeEntityReferenceList(Seq(refTarget.toAttributeEntityReference))
          )
        )
      )
    )

    Await.result(provider.batchUpsertEntities(Source(updates), defaultRequestContext), atMost)

    val ref2 = EntityPointer("typeA", "name2")
    val ref3 = EntityPointer("typeB", "name3")

    // should have called one batch-insert to write the entities
    verify(mockQuery, times(1)).batchCreateEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any(), any())
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
                          0,
                          deleted = false,
                          Some("{}")
      )

    // mocks
    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).existsAll(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
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
                          0,
                          deleted = false,
                          Some("""{"baz":123,"foo":"bar"}""")
      )

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).existsAll(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
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
        0,
        deleted = false,
        Some(
          """{"foo":"bar","ref":{"entityName":"referencedName0","entityType":"referencedType"},"refs":[{"entityName":"referencedName1","entityType":"referencedType"},{"entityName":"referencedName2","entityType":"referencedType"}]}"""
        )
      )

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(true))
    when(mockQuery.createEntity(any(), any())).thenReturn(DBIO.successful(1))
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
      .thenReturn(DBIO.successful(Some(createdEntityRec))) // second request finds the entity we saved

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)

    actual shouldBe entityToCreate

    verify(mockQuery, times(1)).existsAll(
      defaultWorkspace.workspaceIdAsUUID,
      Set(
        EntityPointer("referencedType", "referencedName0"),
        EntityPointer("referencedType", "referencedName1"),
        EntityPointer("referencedType", "referencedName2")
      )
    )
    verify(mockQuery, times(1)).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
    verify(mockQuery, times(2)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
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

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None)) // first request finds nothing
    when(mockQuery.existsAll(any(), any())).thenReturn(DBIO.successful(false)) // reference targets are missing

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = intercept[EntityReferenceNotFoundException] {
      Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)
    }

    actual shouldBe a[EntityReferenceNotFoundException]

    verify(mockQuery, times(1)).existsAll(
      defaultWorkspace.workspaceIdAsUUID,
      Set(
        EntityPointer("referencedType", "referencedName0"),
        EntityPointer("referencedType", "referencedName1"),
        EntityPointer("referencedType", "referencedName2")
      )
    )
    verify(mockQuery, never()).createEntity(any(), any())
    verify(mockQuery, times(1)).getEntity(defaultWorkspace.workspaceIdAsUUID,
                                          entityToCreate.entityType,
                                          entityToCreate.name
    )
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

    val mockQuery = mock[CompactEntityQuery]
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

    verify(mockQuery, never()).existsAll(defaultWorkspace.workspaceIdAsUUID, Set())
    verify(mockQuery, never()).createEntity(defaultWorkspace.workspaceIdAsUUID, entityToCreate)
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
      val mockQuery = mock[CompactEntityQuery]
      val provider = providerWithMocks(mockQuery)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(provider.createEntity(entityToCreate, defaultRequestContext), atMost)
      }

      actual shouldBe a[RawlsExceptionWithErrorReport]

      verify(mockQuery, never()).getEntity(any(), any(), any())
      verify(mockQuery, never()).existsAll(any(), any())
      verify(mockQuery, never()).createEntity(any(), any())
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

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getReferencesTo(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.deleteEntities(any(), any())).thenReturn(DBIO.successful(0))
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

    Await.result(provider.deleteEntities(Seq(entity1.toPointer, entity2.toPointer), defaultRequestContext), atMost)

    verify(mockQuery, times(1)).getReferencesTo(defaultWorkspace.workspaceIdAsUUID,
                                                Seq(entity1.toPointer, entity2.toPointer)
    )
    verify(mockQuery, times(1)).batchHide(defaultWorkspace.workspaceIdAsUUID, Seq(entity1.toPointer, entity2.toPointer))
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

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getReferencesTo(any(), any())).thenReturn(DBIO.successful(Seq(referencingEntity.toPointer)))
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
      Await.result(provider.deleteEntities(Seq(entity1.toPointer, entity2.toPointer), defaultRequestContext), atMost)
    }
    result shouldBe a[DeleteEntitiesConflictException]

    verify(mockQuery, never()).batchHide(defaultWorkspace.workspaceIdAsUUID, Seq(entity1.toPointer, entity2.toPointer))
  }

  behavior of "deleteEntitiesOfType"

  it should "delete all entities of type" in {
    val entityType = "type"

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getReferencesToType(any(), any())).thenReturn(DBIO.successful(Seq()))
    when(mockQuery.deleteEntitiesOfType(any(), any())).thenReturn(DBIO.successful(0))
    when(mockQuery.batchHideType(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    Await.result(provider.deleteEntitiesOfType(entityType, defaultRequestContext), atMost)

    verify(mockQuery, times(1)).getReferencesToType(defaultWorkspace.workspaceIdAsUUID, entityType)
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

    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getReferencesToType(any(), any())).thenReturn(DBIO.successful(Seq(referencingEntity.toPointer)))
    when(mockQuery.batchHideType(any(), any())).thenReturn(DBIO.successful(1))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val result = intercept[DeleteEntitiesOfTypeConflictException] {
      Await.result(provider.deleteEntitiesOfType("type", defaultRequestContext), atMost)
    }

    result shouldBe a[DeleteEntitiesOfTypeConflictException]

    verify(mockQuery, times(1)).getReferencesToType(defaultWorkspace.workspaceIdAsUUID, entity1.entityType)
    verify(mockQuery, never()).batchHideType(defaultWorkspace.workspaceIdAsUUID, entity1.entityType)
  }

  "deleteEntityAttributes" should "have tests" is pending

  behavior of "entityTypeMetadata"

  it should "return empty map when no entities exist" in {
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
    when(mockQuery.getEntity(any[UUID], anyString(), anyString()))
      .thenReturn(DBIO.successful(None))
    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = intercept[EntityNotFoundException] {
      Await.result(provider.getEntity("nonexistent-type", "nonexistent-name", defaultRequestContext), atMost)
    }
    actual shouldBe a[EntityNotFoundException]
  }

  "queryEntities" should "have tests" is pending
  "queryEntitiesSource" should "have tests" is pending

  behavior of "renameAttribute"

  it should "throw error if old and new names are the same" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("same")
    val newName = AttributeName.withDefaultNS("same")

    val mockQueries = mock[CompactEntityQuery]

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[AttributeException] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.code shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error on an invalid new attribute name" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("oldName")
    val newName = AttributeName.withDefaultNS("no! @@bad@@")

    val mockQueries = mock[CompactEntityQuery]

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error on an invalid old attribute name" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("no! @@bad@@")
    val newName = AttributeName.withDefaultNS("newName")

    val mockQueries = mock[CompactEntityQuery]

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error on a reserved new attribute name" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("oldName")
    val newName = AttributeName.withDefaultNS(s"${entityType}_id")

    val mockQueries = mock[CompactEntityQuery]

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error on a reserved old attribute name" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS(s"${entityType}_id")
    val newName = AttributeName.withDefaultNS("newName")

    val mockQueries = mock[CompactEntityQuery]

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error if new name already exists" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("oldName")
    val newName = AttributeName.withDefaultNS("newName")

    val mockQueries = mock[CompactEntityQuery]

    // mock: new name already exists
    when(mockQueries.attributeExists(any[UUID], anyString(), mockitoEq(newName)))
      .thenReturn(DBIO.successful(true))

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[AttributeException] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.code shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "throw error if old name does not exist" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("oldName")
    val newName = AttributeName.withDefaultNS("newName")

    val mockQueries = mock[CompactEntityQuery]

    // mock: new name does not exist
    when(mockQueries.attributeExists(any[UUID], anyString(), mockitoEq(newName)))
      .thenReturn(DBIO.successful(false))
    // mock: old name does not exist
    when(mockQueries.attributeExists(any[UUID], anyString(), mockitoEq(oldName)))
      .thenReturn(DBIO.successful(false))

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[AttributeException] {
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)
    }

    exception.code shouldBe StatusCodes.BadRequest

    // execution should short-circuit before executing a rename
    verify(mockQueries, never()).renameAttribute(any(), any(), any(), any())
  }

  it should "return the number of entities updated if successful" in {
    val entityType = "entityType"
    val oldName = AttributeName.withDefaultNS("oldName")
    val newName = AttributeName.withDefaultNS("newName")

    val mockQueries = mock[CompactEntityQuery]

    // mock: new name does not exist
    when(mockQueries.attributeExists(any[UUID], anyString(), mockitoEq(newName)))
      .thenReturn(DBIO.successful(false))
    // mock: old name does exist
    when(mockQueries.attributeExists(any[UUID], anyString(), mockitoEq(oldName)))
      .thenReturn(DBIO.successful(true))
    // mock: rename touches 123 entities
    when(mockQueries.renameAttribute(any[UUID], anyString(), mockitoEq(oldName), mockitoEq(AttributeRename(newName))))
      .thenReturn(DBIO.successful(123))

    val provider = providerWithMocks(mockQueries)

    val actual =
      Await.result(provider.renameAttribute(entityType, oldName, AttributeRename(newName), testContext), atMost)

    actual shouldBe 123
  }

  behavior of "renameEntity"

  it should "throw BadRequest if the new name is the same as the old name" in {
    val mockQueries = mock[slickDataSource.dataAccess.compactEntityQuery.type]

    val provider = providerWithMocks(mockQueries)

    val entityType = "entityType"
    val oldName = "sameName"
    val newName = "sameName"

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameEntity(entityType, oldName, newName, testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    exception.errorReport.message should include("New name is the same as the entity name")

    verify(mockQueries, never()).renameEntity(any[UUID],
                                              ArgumentMatchers.eq(entityType),
                                              ArgumentMatchers.eq(oldName),
                                              ArgumentMatchers.eq(newName)
    )
  }

  it should "throw BadRequest if the new name is invalid" in {
    val mockQueries = mock[slickDataSource.dataAccess.compactEntityQuery.type]

    val provider = providerWithMocks(mockQueries)

    val entityType = "entityType"
    val oldName = "oldName"
    val newName = "no! @@bad@@"

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameEntity(entityType, oldName, newName, testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    exception.errorReport.message should include("Invalid entity name")

    verify(mockQueries, never()).renameEntity(any[UUID],
                                              ArgumentMatchers.eq(entityType),
                                              ArgumentMatchers.eq(oldName),
                                              ArgumentMatchers.eq(newName)
    )
  }

  it should "throw NotFound if the entity does not exist" in {
    val mockQueries = mock[slickDataSource.dataAccess.compactEntityQuery.type]

    val entityType = "entityType"
    val oldName = "nonExistentName"
    val newName = "newName"

    // The new entity does not exist
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, newName)))))
      .thenReturn(DBIO.successful(false))
    // The old entity does not exist
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, oldName)))))
      .thenReturn(DBIO.successful(false))

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[EntityNotFoundException] {
      Await.result(provider.renameEntity(entityType, oldName, newName, testContext), atMost)
    }

    exception.getMessage should include("Can't find entity name!")

    verify(mockQueries, never()).renameEntity(any[UUID],
                                              ArgumentMatchers.eq(entityType),
                                              ArgumentMatchers.eq(oldName),
                                              ArgumentMatchers.eq(newName)
    )
  }

  it should "throw Conflict if the new entity name already exists" in {
    val mockQueries = mock[slickDataSource.dataAccess.compactEntityQuery.type]

    val entityType = "existingType"
    val oldName = "oldName"
    val newName = "newName"

    // The old entity exists
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, oldName)))))
      .thenReturn(DBIO.successful(true))
    // The new entity already exists
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, newName)))))
      .thenReturn(DBIO.successful(true))

    val provider = providerWithMocks(mockQueries)

    val exception = intercept[DataEntityException] {
      Await.result(provider.renameEntity(entityType, oldName, newName, testContext), atMost)
    }

    exception.code shouldBe StatusCodes.Conflict
    exception.getMessage should include(s"Destination $entityType $newName already exists")

    verify(mockQueries, never()).renameEntity(any[UUID],
                                              ArgumentMatchers.eq(entityType),
                                              ArgumentMatchers.eq(oldName),
                                              ArgumentMatchers.eq(newName)
    )
  }

  it should "successfully rename an entity" in {
    val mockQueries = mock[slickDataSource.dataAccess.compactEntityQuery.type]

    val entityType = "entityType"
    val oldName = "oldName"
    val newName = "newName"

    // The old entity exists
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, oldName)))))
      .thenReturn(DBIO.successful(true))
    // The new entity does not exist
    when(mockQueries.existsAll(any[UUID], ArgumentMatchers.eq(Set(EntityPointer(entityType, newName)))))
      .thenReturn(DBIO.successful(false))

    // The rename operation will update the entity
    when(
      mockQueries.renameEntity(any[UUID],
                               ArgumentMatchers.eq(entityType),
                               ArgumentMatchers.eq(oldName),
                               ArgumentMatchers.eq(newName)
      )
    ).thenReturn(DBIO.successful(1))

    val provider = providerWithMocks(mockQueries)

    val result = Await.result(provider.renameEntity(entityType, oldName, newName, testContext), atMost)

    result shouldBe 1

    verify(mockQueries).renameEntity(any[UUID],
                                     ArgumentMatchers.eq(entityType),
                                     ArgumentMatchers.eq(oldName),
                                     ArgumentMatchers.eq(newName)
    )
  }

  behavior of "renameEntityType"

  it should "throw NotFound if the entity type doesn't exist" in {
    val mockQueries = mock[CompactEntityQuery]

    // Mock the count for a non-existent entity type to return 0
    when(mockQueries.countEntities(any[UUID], anyString())).thenReturn(DBIO.successful(0))

    val provider = providerWithMocks(mockQueries)

    val oldType = "nonExistentType"
    val newType = "newType"

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameEntityType(oldType, EntityTypeRename(newType), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.NotFound
    exception.errorReport.message should include(s"Can't find entity type $oldType")

    verify(mockQueries, never()).renameEntityType(any[UUID], anyString(), anyString())
  }

  it should "throw Conflict if the new entity type already exists" in {
    val mockQueries = mock[CompactEntityQuery]

    // The old type exists
    when(mockQueries.countEntities(any[UUID], mockitoEq("existingType"))).thenReturn(DBIO.successful(2))
    // The new type already exists
    when(mockQueries.countEntities(any[UUID], mockitoEq("alreadyExistsType"))).thenReturn(DBIO.successful(3))

    val provider = providerWithMocks(mockQueries)

    val oldType = "existingType"
    val newType = "alreadyExistsType"

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.renameEntityType(oldType, EntityTypeRename(newType), testContext), atMost)
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.Conflict
    exception.errorReport.message should include(s"$newType already exists as an entity type")

    verify(mockQueries, never()).renameEntityType(any[UUID], anyString(), anyString())
  }

  it should "successfully rename an entity type" in {
    val oldType = "oldType"
    val newType = "newType"

    val mockQueries = mock[CompactEntityQuery]

    // The old type exists
    when(mockQueries.countEntities(any[UUID], mockitoEq(oldType))).thenReturn(DBIO.successful(5))
    // The new type doesn't exist
    when(mockQueries.countEntities(any[UUID], mockitoEq(newType))).thenReturn(DBIO.successful(0))
    // The rename operation will update 5 entities
    when(mockQueries.renameEntityType(any[UUID], mockitoEq(oldType), mockitoEq(newType))).thenReturn(DBIO.successful(5))

    val provider = providerWithMocks(mockQueries)

    val result = Await.result(provider.renameEntityType(oldType, EntityTypeRename(newType), testContext), atMost)

    result shouldBe 5
  }

  behavior of "updateEntity"

  it should "throw if the user specified zero operations" in {
    val entityType = "entityType"
    val entityName = "entityName"
    val operations = Seq.empty[AttributeUpdateOperation]

    val mockQueries = mock[CompactEntityQuery]
    val provider = providerWithMocks(mockQueries)

    val actual = intercept[UnsupportedEntityOperationException] {
      Await.result(provider.updateEntity(entityType, entityName, operations, testContext), atMost)
    }

    actual.code shouldBe StatusCodes.BadRequest
  }

  it should "throw if the entity does not exist" in {
    val entityType = "entityType"
    val entityName = "entityName"
    val operations: Seq[AttributeUpdateOperation] = Seq(
      AddUpdateAttribute(
        AttributeName.withDefaultNS("foo"),
        AttributeNumber(42)
      )
    )

    val mockQueries = mock[CompactEntityQuery]
    // mock: batchUpdate does not find the pre-existing entity
    when(mockQueries.getEntities(mockitoEq(defaultWorkspace.workspaceIdAsUUID), any()))
      .thenAnswer(_ => throw new EntityNotFoundException())
    val provider = providerWithMocks(mockQueries)

    val actual = intercept[EntityNotFoundException] {
      Await.result(provider.updateEntity(entityType, entityName, operations, testContext), atMost)
    }

    actual.code shouldBe StatusCodes.NotFound
  }

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
    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual = provider.findAllReferences(entity)

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
    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual = provider.findAllReferences(entity)

    val expected = Set(
      RefMapping(
        EntityPointer("type", "name"),
        Set(
          EntityPointer("refTypeA", "refName1"),
          EntityPointer("refTypeB", "refName2"),
          EntityPointer("refTypeB", "refName3")
        )
      )
    )

    actual shouldBe expected
  }

  behavior of "findAllReferences(Seq[Entity])"

  it should "return an empty Map when fed an empty Seq" in {
    val input = Seq()

    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual = provider.findAllReferences(input)

    actual shouldBe empty
  }

  it should "return nothing if entities have no references" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
    )
    val entity1 = Entity("name1", "type", attributes)
    val entity2 = Entity("name2", "type", attributes)
    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual =
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
    val entity2 = Entity("name2",
                         "type",
                         Map(
                           AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
                           AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
                         )
    )
    // entity3 has a single reference
    val entity3 = Entity(
      "name3",
      "type",
      Map(
        AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
        AttributeName.withDefaultNS("baz") -> AttributeNumber(42),
        AttributeName.withDefaultNS("ref") -> AttributeEntityReference("refTypeC", "refName4")
      )
    )

    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual =
      provider.findAllReferences(Seq(entity1, entity2, entity3))

    val expected = Set(
      RefMapping(
        entity1.toPointer,
        Set(
          EntityPointer("refTypeA", "refName1"),
          EntityPointer("refTypeB", "refName2"),
          EntityPointer("refTypeB", "refName3")
        )
      ),
      RefMapping(entity3.toPointer, Set(EntityPointer("refTypeC", "refName4")))
    )

    actual shouldBe expected
  }

  it should "respect only the last entity if an entity is repeated in the input Seq" in {
    // repeat the same entity three times in the input, each time with a different reference
    val entity1 = Entity(
      "name1",
      "type",
      Map(
        AttributeName.withDefaultNS("ref1") -> AttributeEntityReference("targetType", "targetName1")
      )
    )
    val entity2 = Entity(
      "name1",
      "type",
      Map(
        AttributeName.withDefaultNS("ref2") -> AttributeEntityReference("targetType", "targetName2")
      )
    )
    val entity3 = Entity(
      "name1",
      "type",
      Map(
        AttributeName.withDefaultNS("ref3") -> AttributeEntityReference("targetType", "targetName3")
      )
    )

    val provider = providerWithMocks(mock[CompactEntityQuery])

    val actual =
      provider.findAllReferences(Seq(entity1, entity2, entity3))

    val expected = Set(
      RefMapping(
        entity1.toPointer,
        Set(
          EntityPointer("targetType", "targetName3")
        )
      )
    )

    actual shouldBe expected
  }

  behavior of "withWorkspaceLastModified"

  it should "trigger a last-modified update on success of the original future" in {
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
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
    val mockQuery = mock[CompactEntityQuery]
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

  behavior of "BatchHandling.applyAll"

  it should "apply subsequent updates to the same non-existent base" in {
    val mockRepository = mock[CompactEntityRepository]

    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1")))
      ),
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1, updated")),
          AddUpdateAttribute(AttributeName.withDefaultNS("col2"), AttributeString("val2"))
        )
      )
    )

    val existingEntitiesByIdentifier: Map[EntityPointer, Entity] = Map(
      EntityPointer("typeA", "some-other-entity") -> Entity(
        "some-other-entity",
        "typeA",
        Map(
          AttributeName.withDefaultNS("existingCol") -> AttributeNumber(42)
        )
      )
    )

    val actual = provider.applyAll(updates, existingEntitiesByIdentifier)
    // The actual result is the entity after all operations are applied to it
    actual shouldBe Seq(
      Entity(
        "name1",
        "typeA",
        Map(AttributeName.withDefaultNS("col1") -> AttributeString("val1, updated"),
            AttributeName.withDefaultNS("col2") -> AttributeString("val2")
        )
      )
    )
  }

  it should "apply subsequent updates to the same existent base" in {
    val mockRepository = mock[CompactEntityRepository]

    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1")))
      ),
      EntityUpdateDefinition(
        "name1",
        "typeA",
        Seq(
          AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1, updated")),
          AddUpdateAttribute(AttributeName.withDefaultNS("col2"), AttributeString("val2"))
        )
      )
    )

    val existingEntitiesByIdentifier: Map[EntityPointer, Entity] = Map(
      EntityPointer("typeA", "name1") -> Entity(
        "name1",
        "typeA",
        Map(
          AttributeName.withDefaultNS("existingCol") -> AttributeNumber(42)
        )
      )
    )

    val actual = provider.applyAll(updates, existingEntitiesByIdentifier)
    // The actual result is the entity after all operations are applied to it
    actual shouldBe Seq(
      Entity(
        "name1",
        "typeA",
        Map(
          AttributeName.withDefaultNS("col1") -> AttributeString("val1, updated"),
          AttributeName.withDefaultNS("col2") -> AttributeString("val2"),
          AttributeName.withDefaultNS("existingCol") -> AttributeNumber(42)
        )
      )
    )
  }

  it should "skip noop updates" in {
    val mockRepository = mock[CompactEntityRepository]

    // provider using mocks
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

    val updates: Seq[EntityUpdateDefinition] = Seq(
      EntityUpdateDefinition("name1",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val1")))
      ),
      EntityUpdateDefinition("name2",
                             "typeA",
                             Seq(AddUpdateAttribute(AttributeName.withDefaultNS("col1"), AttributeString("val2")))
      )
    )

    val existingEntitiesByIdentifier: Map[EntityPointer, Entity] = Map(
      EntityPointer("typeA", "name1") -> Entity("name1",
                                                "typeA",
                                                Map(
                                                  AttributeName.withDefaultNS("col1") -> AttributeString("val1")
                                                )
      ),
      EntityPointer("typeA", "name2") -> Entity("name2",
                                                "typeA",
                                                Map(
                                                  AttributeName.withDefaultNS("col1") -> AttributeString("val1")
                                                )
      )
    )

    val actual = provider.applyAll(updates, existingEntitiesByIdentifier)
    // The actual result is the entity after all operations are applied to it; the noop operations are skipped
    actual shouldBe Seq(
      Entity("name2",
             "typeA",
             Map(
               AttributeName.withDefaultNS("col1") -> AttributeString("val2")
             )
      )
    )
  }

  behavior of "copyEntities"

  it should "copy entities from source workspace to destination workspace when no conflicts are present" in {
    val config = CompactEntityProviderConfig()
    val mockQuery = mock[CompactEntityQuery]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))

    val sourceWorkspaceId = UUID.randomUUID
    val sourceWorkspace = Workspace("namespace",
                                    "sourceWorkspace",
                                    sourceWorkspaceId.toString,
                                    "source-bucket",
                                    None,
                                    new DateTime(),
                                    new DateTime(),
                                    "creator",
                                    Map.empty
    )
    val destWorkspace = Workspace("namespace",
                                  "destWorkspace",
                                  UUID.randomUUID.toString,
                                  "dest-bucket",
                                  None,
                                  new DateTime(),
                                  new DateTime(),
                                  "creator",
                                  Map.empty
    )

    val entityType = "sampleType"
    val entityNames = Seq("entity1", "entity2", "entity3")
    val entitiesToCopyRefs = entityNames.map(name => EntityPointer(entityType, name)).toSet

    val mockEntityRefs = entityNames.map(name => EntityPointer(entityType, name))
    when(mockQuery.getEntityRefs(any[UUID], any[Set[EntityPointer]])).thenReturn(DBIO.successful(Seq.empty))
    when(mockQuery.recursiveGetEntityReferences(sourceWorkspaceId, entitiesToCopyRefs, config.batchCopyBatchSize))
      .thenReturn(
        DBIO.successful(
          Set(
            RefMapping(EntityPointer(entityType, "entity1"), Set.empty),
            RefMapping(EntityPointer(entityType, "entity2"), Set.empty),
            RefMapping(EntityPointer(entityType, "entity3"), Set.empty)
          )
        )
      )
    when(mockQuery.copyEntitiesToNewWorkspace(any[UUID], any[UUID], any[Set[EntityPointer]], any[Int]))
      .thenReturn(DBIO.successful(3))

    val provider =
      providerWithMocks(mockRepository, EntityRequestArguments(sourceWorkspace, defaultRequestContext), config)
    val result = Await.result(provider.copyEntities(sourceWorkspace,
                                                    destWorkspace,
                                                    entityType,
                                                    entityNames,
                                                    linkExistingEntities = false,
                                                    defaultRequestContext
                              ),
                              atMost
    )

    result.entitiesCopied.length shouldBe 3
    result.hardConflicts shouldBe empty
    result.softConflicts shouldBe empty

    verify(mockQuery, times(1)).getEntityRefs(destWorkspace.workspaceIdAsUUID, mockEntityRefs.toSet)
    verify(mockQuery, times(1)).copyEntitiesToNewWorkspace(sourceWorkspace.workspaceIdAsUUID,
                                                           destWorkspace.workspaceIdAsUUID,
                                                           mockEntityRefs.toSet,
                                                           config.batchCopyBatchSize
    )
  }

  it should "not copy entities when a hard conflict is present" in {
    val mockQuery = mock[CompactEntityQuery]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))

    val sourceWorkspace = Workspace("namespace",
                                    "sourceWorkspace",
                                    UUID.randomUUID.toString,
                                    "source-bucket",
                                    None,
                                    new DateTime(),
                                    new DateTime(),
                                    "creator",
                                    Map.empty
    )
    val destWorkspace = Workspace("namespace",
                                  "destWorkspace",
                                  UUID.randomUUID.toString,
                                  "dest-bucket",
                                  None,
                                  new DateTime(),
                                  new DateTime(),
                                  "creator",
                                  Map.empty
    )

    val entityType = "sampleType"
    val entityNames = Seq("entity1")

    val mockEntityRefs = entityNames.map(name => EntityPointer(entityType, name))
    when(mockQuery.getEntityRefs(any[UUID], any[Set[EntityPointer]]))
      .thenReturn(DBIO.successful(Seq(CompactEntityRefRecord(1, "entity1", entityType))))
    val provider = providerWithMocks(mockRepository, EntityRequestArguments(sourceWorkspace, defaultRequestContext))

    val result = Await.result(provider.copyEntities(sourceWorkspace,
                                                    destWorkspace,
                                                    entityType,
                                                    entityNames,
                                                    linkExistingEntities = false,
                                                    defaultRequestContext
                              ),
                              atMost
    )
    result.entitiesCopied shouldBe empty
    result.hardConflicts shouldBe Seq(EntityHardConflict(entityType, "entity1"))
    result.softConflicts shouldBe empty

    verify(mockQuery, times(1)).getEntityRefs(destWorkspace.workspaceIdAsUUID, mockEntityRefs.toSet)
    verify(mockQuery, times(0)).copyEntitiesToNewWorkspace(sourceWorkspace.workspaceIdAsUUID,
                                                           destWorkspace.workspaceIdAsUUID,
                                                           mockEntityRefs.toSet
    )

  }

  it should "not copy entities when soft conflicts are present and linkExistingEntities is false" in {
    val config = CompactEntityProviderConfig()
    val mockQuery = mock[CompactEntityQuery]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))

    val sourceWorkspace = Workspace("namespace",
                                    "sourceWorkspace",
                                    UUID.randomUUID.toString,
                                    "source-bucket",
                                    None,
                                    new DateTime(),
                                    new DateTime(),
                                    "creator",
                                    Map.empty
    )
    val destWorkspace = Workspace("namespace",
                                  "destWorkspace",
                                  UUID.randomUUID.toString,
                                  "dest-bucket",
                                  None,
                                  new DateTime(),
                                  new DateTime(),
                                  "creator",
                                  Map.empty
    )

    val entityType = "sampleType"
    val entityNames = Seq("entity1")
    val entitiesToCopyRefs = entityNames.map(name => EntityPointer(entityType, name)).toSet

    val mockEntityRefs = entityNames.map(name => EntityPointer(entityType, name))
    when(mockQuery.getEntityRefs(destWorkspace.workspaceIdAsUUID, Set(EntityPointer(entityType, "entity1"))))
      .thenReturn(DBIO.successful(Seq()))
    when(mockQuery.getEntityRefs(destWorkspace.workspaceIdAsUUID, Set(EntityPointer(entityType, "entity2"))))
      .thenReturn(DBIO.successful(Seq(CompactEntityRefRecord(2, "entity2", entityType))))
    when(
      mockQuery.recursiveGetEntityReferences(sourceWorkspace.workspaceIdAsUUID,
                                             entitiesToCopyRefs,
                                             config.batchCopyBatchSize
      )
    )
      .thenReturn(
        DBIO.successful(
          Set(RefMapping(EntityPointer(entityType, "entity1"), Set(EntityPointer(entityType, "entity2"))))
        )
      )

    val provider = providerWithMocks(mockRepository, EntityRequestArguments(sourceWorkspace, defaultRequestContext))
    val result = Await.result(provider.copyEntities(sourceWorkspace,
                                                    destWorkspace,
                                                    entityType,
                                                    entityNames,
                                                    linkExistingEntities = false,
                                                    defaultRequestContext
                              ),
                              atMost
    )

    result.entitiesCopied shouldBe empty
    result.hardConflicts shouldBe empty
    result.softConflicts shouldBe Seq(
      EntitySoftConflict(entityType, "entity1", Seq(EntitySoftConflict(entityType, "entity2", Seq.empty)))
    )

    verify(mockQuery, times(1)).getEntityRefs(destWorkspace.workspaceIdAsUUID, mockEntityRefs.toSet)
    verify(mockQuery, times(0)).copyEntitiesToNewWorkspace(sourceWorkspace.workspaceIdAsUUID,
                                                           destWorkspace.workspaceIdAsUUID,
                                                           mockEntityRefs.toSet
    )
  }

  it should "exclude copying soft conflicts when soft conflicts are present and linkExistingEntities is true" in {
    val config = CompactEntityProviderConfig()
    val mockQuery = mock[CompactEntityQuery]
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries).thenReturn(mockQuery)
    when(mockRepository.dataSource).thenReturn(slickDataSource)
    when[ReadWriteAction[Int]](mockRepository.updateLastModified(any[UUID]()))
      .thenReturn(DBIO.successful(1))

    val sourceWorkspace = Workspace("namespace",
                                    "sourceWorkspace",
                                    UUID.randomUUID.toString,
                                    "source-bucket",
                                    None,
                                    new DateTime(),
                                    new DateTime(),
                                    "creator",
                                    Map.empty
    )
    val destWorkspace = Workspace("namespace",
                                  "destWorkspace",
                                  UUID.randomUUID.toString,
                                  "dest-bucket",
                                  None,
                                  new DateTime(),
                                  new DateTime(),
                                  "creator",
                                  Map.empty
    )

    val entityType = "sampleType"
    val entityNames = Seq("entity1")
    val entitiesToCopyRefs = entityNames.map(name => EntityPointer(entityType, name)).toSet

    val mockEntityRefs = entityNames.map(name => EntityPointer(entityType, name))
    when(mockQuery.getEntityRefs(destWorkspace.workspaceIdAsUUID, Set(EntityPointer(entityType, "entity1"))))
      .thenReturn(DBIO.successful(Seq()))
    when(mockQuery.getEntityRefs(destWorkspace.workspaceIdAsUUID, Set(EntityPointer(entityType, "entity2"))))
      .thenReturn(DBIO.successful(Seq(CompactEntityRefRecord(2, "entity2", entityType))))
    when(
      mockQuery.recursiveGetEntityReferences(sourceWorkspace.workspaceIdAsUUID,
                                             entitiesToCopyRefs,
                                             config.batchCopyBatchSize
      )
    )
      .thenReturn(
        DBIO.successful(
          Set(RefMapping(EntityPointer(entityType, "entity1"), Set(EntityPointer(entityType, "entity2"))))
        )
      )
    when(mockQuery.copyEntitiesToNewWorkspace(any[UUID], any[UUID], any[Set[EntityPointer]], any[Int]))
      .thenReturn(DBIO.successful(1))

    val provider =
      providerWithMocks(mockRepository, EntityRequestArguments(sourceWorkspace, defaultRequestContext), config)
    val result = Await.result(provider.copyEntities(sourceWorkspace,
                                                    destWorkspace,
                                                    entityType,
                                                    entityNames,
                                                    linkExistingEntities = true,
                                                    defaultRequestContext
                              ),
                              atMost
    )

    result.entitiesCopied.length shouldBe 1
    result.hardConflicts shouldBe empty
    result.softConflicts shouldBe empty

    verify(mockQuery, times(1)).getEntityRefs(destWorkspace.workspaceIdAsUUID, mockEntityRefs.toSet)
    verify(mockQuery, times(1)).copyEntitiesToNewWorkspace(sourceWorkspace.workspaceIdAsUUID,
                                                           destWorkspace.workspaceIdAsUUID,
                                                           mockEntityRefs.toSet,
                                                           config.batchCopyBatchSize
    )
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
