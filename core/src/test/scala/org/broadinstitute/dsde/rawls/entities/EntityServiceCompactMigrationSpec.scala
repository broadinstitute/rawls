package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Sink
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.{QuicksilverMigrationResult, RawSqlQuery, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactoryImpl,
  MockBigQueryServiceFactory,
  MockGoogleServicesDAO,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository,
  CompactEntitySerialization
}
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.CompactDataTables
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeBoolean,
  AttributeEntityReference,
  AttributeEntityReferenceEmptyList,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  AttributeValueRawJson,
  Entity,
  RawlsRequestContext,
  RawlsUser,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.broadinstitute.dsde.rawls.workspace.{
  WorkspaceRepository,
  WorkspaceSettingRepository,
  WorkspaceSettingService
}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.mockito.Mockito.RETURNS_SMART_NULLS
import org.scalatest.Inspectors.forEvery
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import slick.jdbc.TransactionIsolation
import spray.json._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}

class EntityServiceCompactMigrationSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with TestDriverComponent
    with RawlsTestUtils
    with Eventually
    with ScalaFutures
    with MockitoTestUtils
    with RawlsStatsDTestUtils
    with CompactEntitySerialization
    with RawSqlQuery {

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit
    val executionContext: ExecutionContext
  ) extends EntityApiService
      with MockUserInfoDirectivesWithUser {
    private val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    lazy val entityService: EntityService = entityServiceConstructor(ctx1)

    def actorRefFactory = system
    val samDAO = new MockSamDAO(dataSource)(executionContext)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactoryImpl = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val googleStorageService = mock[GoogleStorageService[IO]](RETURNS_SMART_NULLS)

    val workspaceSettingRepository = new WorkspaceSettingRepository(dataSource)

    val workspaceSettingServiceConstructor: Option[RawlsRequestContext => WorkspaceSettingService] = Some { ctx =>
      new WorkspaceSettingService(
        ctx,
        workspaceSettingRepository,
        new WorkspaceRepository(dataSource),
        new MockGoogleServicesDAO("groupsPrefix"),
        samDAO,
        googleStorageService,
        entityService
      )(executionContext, global)
    }

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      EntityManager.defaultEntityManager(
        dataSource,
        workspaceSettingRepository,
        testConf.getBoolean("entityStatisticsCache.enabled"),
        testConf.getDuration("entities.queryTimeout"),
        workbenchMetricBaseName
      )(executionContext, system),
      7, // <-- specifically, chosen to be lower than the number of samples in "workspace" within testData
      Option(workspaceSettingRepository)
    ) _
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withLegacyDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: TestApiService => T) = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  private val atMost = Duration("60 seconds") // timeout for Await() in tests

  private val testWorkspaces = Map(
    legacyTestData.workspace -> 18, // has 20 entities, but 2 of them have no attributes, so 18 entities are updated in the migration
    legacyTestData.workspaceNoAttrs -> 0, // has 0 entities
    legacyTestData.workspaceWithRealm -> 0 // has 1 entity, but that entity has no attributes, so 0 entities are updated in the migration
  )

  behavior of "Compact Entity Migration"
  testWorkspaces.foreach { case (workspace, expectedCount) =>
    it should s"migrate to compact entities for workspace ${workspace.toWorkspaceName}" in withTestDataServices {
      apiService =>
        // entity data is already loaded into legacy tables via withTestDataServices

        // perform migration - this keeps legacy attributes and adds Quicksilver attributes
        // set a low batch size to ensure we exercise the batching logic
        val migrationResult =
          Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName, batchSize = 3), atMost)

        migrationResult shouldBe QuicksilverMigrationResult(expectedCount, 0, 0)

        val defaultRequestContext =
          RawlsRequestContext(
            UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
          )

        val requestArguments = EntityRequestArguments(workspace, defaultRequestContext)

        // get providers
        val localProvider =
          new LocalEntityProvider(requestArguments,
                                  slickDataSource,
                                  true,
                                  java.time.Duration.ofSeconds(60),
                                  workbenchMetricBaseName
          )

        val compactProvider = new CompactEntityProvider(requestArguments,
                                                        new CompactEntityRepository(slickDataSource),
                                                        "testMetricPrefix",
                                                        CompactEntityProviderConfig()
        )

        // get all entity types
        val localEntityTypes =
          Await.result(localProvider.entityTypeMetadata(useCache = true, defaultRequestContext), atMost).keys

        forEvery(localEntityTypes) { entityType =>
          withClue(s"for type $entityType") {
            // get all entities of this type. This materializes the result; don't run this on large workspaces
            val localEntities =
              Await.result(localProvider.listEntities(entityType).runWith(Sink.seq), atMost)
            val compactEntities =
              Await.result(compactProvider.listEntities(entityType).runWith(Sink.seq), atMost)

            compactEntities shouldBe localEntities
          }
        }
    }
  }

  it should s"migrate to compact entities with various attribute data types" in withTestDataServices { apiService =>
    val workspace = legacyTestData.workspace // has some entities we can use to test references

    // various attribute types to ensure migration works for all of them
    val attrs: Map[AttributeName, Attribute] = Map(
      AttributeName.withDefaultNS("stringAttr") -> AttributeString("stringValue"),
      AttributeName.withLibraryNS("nullAttr") -> AttributeNull,
      AttributeName.fromDelimitedName("import:intAttr") -> AttributeNumber(Long.MaxValue),
      AttributeName.fromDelimitedName("pfb:booleanAttr") -> AttributeBoolean(true),
      AttributeName.fromDelimitedName("othernamespace:jsonObjAttr") -> AttributeValueRawJson(
        """{"foo":"bar", "nested": {"stuff": [2,3,4,false]}}""".parseJson
      ),
      AttributeName.withDefaultNS("jsonArrAttr") -> AttributeValueRawJson("""[1,2,3,[4,5,6],[7,8,9]]""".parseJson),
      /* This migrates as an AttributeString, not AttributeValueRawJson. We are ok with that.
      AttributeName.withDefaultNS("jsonStrAttr") -> AttributeValueRawJson(
        """"this is a string parsed as json"""".parseJson
      ),
       */
      AttributeName.withDefaultNS("refAttr") -> AttributeEntityReference(testData.sample1.entityType,
                                                                         testData.sample1.name
      ),
      AttributeName.withDefaultNS("duplicateRefAttr") -> AttributeEntityReference(testData.sample1.entityType,
                                                                                  testData.sample1.name
      ), // same as previous, to test de-duplication when inserting to ENTITY_REFS
      AttributeName.withDefaultNS("emptyList") -> AttributeValueEmptyList,
      // This migrates as an AttributeValueEmptyList. We are ok with that; both result in `[]`
      // AttributeName.withDefaultNS("emptyRefList") -> AttributeEntityReferenceEmptyList,
      AttributeName.withDefaultNS("valueList") -> AttributeValueList(
        Seq(
          AttributeNumber(Long.MinValue),
          AttributeNumber(-1L),
          AttributeNumber(0L),
          AttributeNumber(1L),
          AttributeNumber(Long.MaxValue)
        )
      ),
      AttributeName.withDefaultNS("refList") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference(testData.sample2.entityType, testData.sample2.name),
          AttributeEntityReference(testData.sample1.entityType, testData.sample1.name),
          AttributeEntityReference(testData.sample3.entityType, testData.sample3.name)
        )
      )
    )

    val entity = Entity("lotsaDataTypes", "willThisWork", attrs)

    val defaultRequestContext =
      RawlsRequestContext(
        UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
      )

    val requestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    // get providers
    val localProvider =
      new LocalEntityProvider(requestArguments,
                              slickDataSource,
                              true,
                              java.time.Duration.ofSeconds(60),
                              workbenchMetricBaseName
      )

    val compactProvider = new CompactEntityProvider(requestArguments,
                                                    new CompactEntityRepository(slickDataSource),
                                                    "testMetricPrefix",
                                                    CompactEntityProviderConfig()
    )

    // save the entity with the various attributes
    val savedEntity = Await.result(localProvider.createEntity(entity, defaultRequestContext), atMost)
    savedEntity shouldBe entity

    // perform migration - this keeps legacy attributes and adds Quicksilver attributes
    val migrationResult =
      Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName), atMost)

    migrationResult shouldBe QuicksilverMigrationResult(19, 0, 0) // 18 from the test data, plus the one we just created

    val compactEntity =
      Await.result(compactProvider.getEntity(entity.entityType, entity.name, defaultRequestContext), atMost)

    val localEntity =
      Await.result(localProvider.getEntity(entity.entityType, entity.name, defaultRequestContext), atMost)

    forEvery(localEntity.attributes.keys) { attributeName =>
      withClue(s"for attribute $attributeName") {
        // check that the attributes match
        compactEntity.attributes.get(attributeName) shouldBe localEntity.attributes.get(attributeName)
      }
    }

  }

  it should s"maintain reference array ordering" in withTestDataServices { apiService =>
    val workspace = legacyTestData.workspace // has some entities we can use to test references

    val targetEntityType = "targetEntityType"
    val targetEntityNames = Seq("targetName1", "targetName2", "targetName3", "targetName4", "targetName5")
    val targetEntities = targetEntityNames.map { name =>
      Entity(name, targetEntityType, Map.empty)
    }
    val targetReferences = targetEntities.map(_.toReference)

    // Build an attribute map for our source entity with lots of entity reference lists
    // with randomized naming. The randomization here is used to ensure that nothing implicitly relies
    // on ordering of attribute names, entity names, or entity types.
    //
    // Randomization means there is a chance of false positives, but over time also gives us more breadth of test cases
    val numAttrsToTest = 100
    val minReferencesPerList = 5
    val maxReferencesPerList = 50
    val chanceOfScalarAttribute = 0.1 // 10% chance of a scalar attribute, otherwise an entity reference list
    val randomGenerator = RandomStringUtils.insecure() // we don't care about crypto-level security here
    val attrs: AttributeMap = (Range.inclusive(1, numAttrsToTest) map { _ =>
      val attrName = AttributeName.fromDelimitedName(
        s"${randomGenerator.nextAlphabetic(4, 8)}:${randomGenerator.nextAlphanumeric(8, 20)}"
      ) // random attribute name
      val isScalar = scala.util.Random.nextDouble() < chanceOfScalarAttribute
      val attrValue = if (isScalar) {
        // scalar attribute
        targetReferences(scala.util.Random.nextInt(targetReferences.length))
      } else {
        val refs = Range(0, scala.util.Random.between(minReferencesPerList, maxReferencesPerList)).map { _ =>
          targetReferences(scala.util.Random.nextInt(targetReferences.length))
        }
        AttributeEntityReferenceList(refs)
      }
      attrName -> attrValue
    }).toMap

    val entity = Entity("lotsaDataTypes", "willThisWork", attrs)

    val defaultRequestContext =
      RawlsRequestContext(
        UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
      )

    val requestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    // get providers
    val localProvider =
      new LocalEntityProvider(requestArguments,
                              slickDataSource,
                              true,
                              java.time.Duration.ofSeconds(60),
                              workbenchMetricBaseName
      )

    val compactProvider = new CompactEntityProvider(requestArguments,
                                                    new CompactEntityRepository(slickDataSource),
                                                    "testMetricPrefix",
                                                    CompactEntityProviderConfig()
    )

    // save target entities to use as references
    targetEntities.foreach { target =>
      Await.result(localProvider.createEntity(target, defaultRequestContext), atMost)
    }

    // save the entity with the randomized references
    val savedEntity = Await.result(localProvider.createEntity(entity, defaultRequestContext), atMost)
    savedEntity shouldBe entity

    // perform migration - this keeps legacy attributes and adds Quicksilver attributes
    val migrationResult =
      Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName), atMost)

    migrationResult shouldBe QuicksilverMigrationResult(19, 0, 0) // 18 from the test data, plus the one we just created

    val compactEntity =
      Await.result(compactProvider.getEntity(entity.entityType, entity.name, defaultRequestContext), atMost)

    val localEntity =
      Await.result(localProvider.getEntity(entity.entityType, entity.name, defaultRequestContext), atMost)

    forEvery(localEntity.attributes.keys) { attributeName =>
      withClue(s"for attribute $attributeName") {
        // check that the attributes match
        compactEntity.attributes.get(attributeName) shouldBe localEntity.attributes.get(attributeName)
      }
    }

  }

  it should s"hard delete legacy data when requested" in withTestDataServices { apiService =>
    val workspace = legacyTestData.workspace // has some entities we can use to test references

    val defaultRequestContext =
      RawlsRequestContext(
        UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
      )

    val requestArguments = EntityRequestArguments(workspace, defaultRequestContext)

    // get providers
    val localProvider =
      new LocalEntityProvider(requestArguments,
                              slickDataSource,
                              true,
                              java.time.Duration.ofSeconds(60),
                              workbenchMetricBaseName
      )

    // Attempt to soft-delete two existing entities. This will actually only delete indiv2, since indiv1 is referenced
    // by a submission.
    val softDeletes = Seq(testData.indiv1.toPointer, testData.indiv2.toPointer)
    val softDeleteResult = Await.result(localProvider.deleteEntities(softDeletes, defaultRequestContext), atMost)
    softDeleteResult shouldBe 2

    // perform migration with cleanup - this deletes legacy attributes and hard-deleted soft-deleted entities
    val migrationResult =
      Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName, cleanup = true), atMost)

    migrationResult shouldBe QuicksilverMigrationResult(16, 1, 36)
    // numEntitiesUpdated: test data has 18, but we soft-deleted 2
    // numEntitiesDeleted: 1 soft-deleted entity were hard-deleted
    // numAttributesDeleted: we delete all attributes in the workspace, not just the soft-deleted ones
  }

  it should s"roll back the CompactDataTables setting on failed migration" in withTestDataServices { apiService =>
    val workspace = legacyTestData.workspace // has some entities we can use to test references

    /** Locks all entities of the given workspace for ${lockSeconds} seconds. Use this to simulate database contention. */
    def lockAllEntities(dataSource: SlickDataSource, workspaceId: UUID, lockSeconds: Int): Future[Unit] = {
      import dataSource.dataAccess.driver.api._
      val locker = for {
        _ <- sql"""select * from ENTITY where workspace_id = $workspaceId for update;""".as[Unit]
        _ <- sql"""select sleep(${lockSeconds + 2});""".as[Unit]
      } yield ()
      dataSource.database.run(locker.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))
    }

    // lock entities; do NOT wait for this Future to complete so it is still running when we kick off the migration
    lockAllEntities(slickDataSource, workspace.workspaceIdAsUUID, 60)
    // ask to perform migration
    val migrationResult =
      Try(Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName), atMost))

    migrationResult shouldBe a[Failure[_]]

    // check if the setting exists
    val repo = new WorkspaceSettingRepository(slickDataSource)
    Await.result(repo.hasPendingSettings(workspace.workspaceIdAsUUID, CompactDataTables), Duration.Inf) shouldBe false
    Await.result(repo.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, CompactDataTables),
                 Duration.Inf
    ) shouldBe empty

  }

}
