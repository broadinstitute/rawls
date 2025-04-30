package org.broadinstitute.dsde.rawls.entities.compact

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  CompactEntityQuery,
  CompactEntityRecord,
  EntityTypeAndAttributeKey,
  EntityTypeAndCount,
  ReadWriteAction,
  TestDriverComponentWithFlatSpecAndMatchers
}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityNotFoundException, EntityReferenceNotFoundException}
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNumber,
  AttributeString,
  Entity,
  EntityTypeMetadata,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, timeout => mockitotimeout, times, verify, when}
import org.scalatest.concurrent.Futures.{scaled, PatienceConfig}
import org.scalatest.time.{Millis, Seconds, Span}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CompactEntityProviderSpec extends TestDriverComponentWithFlatSpecAndMatchers with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

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
  "batchUpsertEntities" should "have tests" is pending
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
    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, never()).upsertReferences(any(), any())
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
    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, never()).upsertReferences(any(), any())
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
    when(mockQuery.upsertReferences(any(), any()))
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
    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, times(1)).upsertReferences(42,
                                                 Set(0, 1, 2)
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
    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, never()).upsertReferences(any(), any())
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
    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, never()).upsertReferences(any(), any())
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
      verify(mockQuery, never()).deleteReferences(any(), any())
      verify(mockQuery, never()).upsertReferences(any(), any())
    }
  }

  "deleteEntities" should "have tests" is pending
  "deleteEntitiesOfType" should "have tests" is pending
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

  behavior of "findAllReferences"

  it should "return nothing if entity has no references" in {
    val attributes = Map(
      AttributeName.withDefaultNS("foo") -> AttributeString("bar"),
      AttributeName.withDefaultNS("baz") -> AttributeNumber(42)
    )
    val entity = Entity("name", "type", attributes)
    val provider = providerWithMocks(mock[slickDataSource.dataAccess.compactEntityQuery.type])

    val actual: Map[AttributeName, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

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

    val actual: Map[AttributeName, Seq[AttributeEntityReference]] = provider.findAllReferences(entity)

    val expected = Map(
      AttributeName.withDefaultNS("ref") -> Seq(AttributeEntityReference("refTypeA", "refName1")),
      AttributeName.withDefaultNS("reflist") ->
        Seq(
          AttributeEntityReference("refTypeB", "refName2"),
          AttributeEntityReference("refTypeB", "refName3")
        )
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

    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, never()).upsertReferences(any(), any())
  }

  it should "skip deletes when isInsert=true and non-empty references" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.upsertReferences(any(), any()))
      .thenAnswer { invocation =>
        DBIO.successful(invocation.getArgument[Set[Long]](1).size)
      }
    when(mockQuery.deleteReferences(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(7, 8, 9), isInsert = true), atMost)

    actual shouldBe (0, 3)

    verify(mockQuery, never()).deleteReferences(any(), any())
    verify(mockQuery, times(1)).upsertReferences(2, Set(7, 8, 9))
  }

  it should "skip upserts when isInsert=false and references are empty" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.deleteReferences(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))
    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(), isInsert = false), atMost)

    actual shouldBe (Int.MinValue, 0)

    verify(mockQuery, times(1)).deleteReferences(2, Set())
    verify(mockQuery, never()).upsertReferences(any(), any())
  }

  it should "both delete and upsert when isInsert=false and non-empty references" in {
    // mocks
    val mockQuery = mock[slickDataSource.dataAccess.compactEntityQuery.type]
    when(mockQuery.upsertReferences(any(), any()))
      .thenAnswer { invocation =>
        DBIO.successful(invocation.getArgument[Set[Long]](1).size)
      }
    when(mockQuery.deleteReferences(any(), any()))
      .thenReturn(DBIO.successful(Int.MinValue))

    // provider using mocks
    val provider = providerWithMocks(mockQuery)

    val actual = runAndWait(provider.replaceReferences(2, Set(7, 8, 9), isInsert = false), atMost)

    actual shouldBe (Int.MinValue, 3)

    verify(mockQuery, times(1)).deleteReferences(2, Set(7, 8, 9))
    verify(mockQuery, times(1)).upsertReferences(2, Set(7, 8, 9))
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
    val provider = providerWithMocks(mockRepository, defaultEntityRequestArguments)

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

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================
  private def providerWithMocks(queries: CompactEntityQuery,
                                requestArguments: EntityRequestArguments = defaultEntityRequestArguments
  ): CompactEntityProvider = {
    val mockRepository = mock[CompactEntityRepository]
    when(mockRepository.queries)
      .thenReturn(queries)
    when(mockRepository.dataSource).thenReturn(slickDataSource)

    providerWithMocks(mockRepository, requestArguments)
  }

  private def providerWithMocks(repository: CompactEntityRepository,
                                requestArguments: EntityRequestArguments
  ): CompactEntityProvider =
    new CompactEntityProvider(requestArguments, repository)(ec)

}
