package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Sink
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactoryImpl,
  MockBigQueryServiceFactory,
  MockGoogleServicesDAO,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository
}
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityProvider
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.{
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
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class EntityServiceCompactMigrationSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with TestDriverComponent
    with RawlsTestUtils
    with Eventually
    with ScalaFutures
    with MockitoTestUtils
    with RawlsStatsDTestUtils {

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

    val workspaceSettingServiceConstructor: Option[RawlsRequestContext => WorkspaceSettingService] = Some { ctx =>
      new WorkspaceSettingService(
        ctx,
        new WorkspaceSettingRepository(dataSource),
        new WorkspaceRepository(dataSource),
        new MockGoogleServicesDAO("groupsPrefix"),
        samDAO,
        googleStorageService
      )(executionContext, global)
    }

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      EntityManager.defaultEntityManager(
        dataSource,
        new WorkspaceSettingRepository(dataSource),
        testConf.getBoolean("entityStatisticsCache.enabled"),
        testConf.getDuration("entities.queryTimeout"),
        workbenchMetricBaseName
      )(executionContext, system),
      7, // <-- specifically, chosen to be lower than the number of samples in "workspace" within testData
      workspaceSettingServiceConstructor
    ) _
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: TestApiService => T) = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }

  /**
   * Returns a Seq containing all members of testData that are of type Workspace
   * Uses reflection to find all Workspace fields in testData
   */
  def getAllWorkspaces: Seq[Workspace] = {
    import scala.reflect.runtime.universe._

    val mirror = runtimeMirror(testData.getClass.getClassLoader)
    val instanceMirror = mirror.reflect(testData)
    val testDataType = instanceMirror.symbol.toType

    testDataType.decls.collect {
      case m: MethodSymbol if m.isGetter && m.returnType <:< typeOf[Workspace] =>
        instanceMirror.reflectMethod(m).apply().asInstanceOf[Workspace]
    }.toSeq
  }

  private val atMost = Duration("60 seconds") // timeout for Await() in tests

  behavior of "Compact Entity Migration"

//  val testWorkspaces: Seq[Workspace] = Seq(testData.workspace, testData.workspaceLocked, testData.regionalWorkspace, testData.workspaceNoAttrs,
//    testData.workspaceWithRealm, testData.workspaceWithMultiGroupAD, testData.controlledWorkspace,
//    testData.otherWorkspaceWithRealm, testData.workspaceNoSubmissions, testData.workspaceNoEntities,
//    testData.workspaceSuccessfulSubmission, testData.workspaceFailedSubmission)

  getAllWorkspaces.filterNot(_.name.contains("azure")).foreach { workspace =>
    it should s"migrate to compact entities for workspace ${workspace.toWorkspaceName}" in withTestDataServices {
      apiService =>
        // entity data is already loaded into legacy tables via withTestDataServices

        // perform migration - this keeps legacy attributes and adds Quicksilver attributes
        Await.result(apiService.entityService.quicksilverMigration(workspace.toWorkspaceName), atMost)

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
                                                        CompactEntityProviderConfig()
        )

        // get all entity types
        val localEntityTypes =
          Await.result(localProvider.entityTypeMetadata(useCache = true, defaultRequestContext), atMost)
        localEntityTypes.keys.foreach { entityType =>
          withClue(s"for type $entityType") {
            // get all entities of this type
            val localEntities =
              Await.result(localProvider.listEntities(entityType).runWith(Sink.seq), atMost)
            val compactEntities =
              Await.result(compactProvider.listEntities(entityType).runWith(Sink.seq), atMost)

            compactEntities shouldBe localEntities
          }
        }
    }

  }
}
