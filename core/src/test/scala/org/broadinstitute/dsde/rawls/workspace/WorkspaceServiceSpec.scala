package org.broadinstitute.dsde.rawls.workspace

import akka.actor.PoisonPill
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import bio.terra.policy.model.{TpsPaoGetResult, TpsPolicyInput, TpsPolicyInputs, TpsPolicyPair}
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{
  AzureContext,
  GcpContext,
  WorkspaceDescription,
  WorkspaceStageModel,
  WsmPolicyInput,
  WsmPolicyInputs,
  WsmPolicyPair
}
import cats.implicits.catsSyntaxOptionId
import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.iam.v1.model.Role
import com.google.cloud.Identity
import com.google.cloud.storage.StorageException
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAOImpl, BillingRepository}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.tps.TpsDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.fastpass.{FastPassServiceImpl, MockFastPassService}
import org.broadinstitute.dsde.rawls.genomics.GenomicsServiceImpl
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationService
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.policy.PolicyService
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferServiceImpl
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterServiceImpl
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.{
  NoSuchWorkspaceException,
  RawlsExceptionWithErrorReport,
  RawlsTestUtils,
  TestExecutionContext
}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, IamPermission}
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchGroupName, WorkbenchUserId}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import spray.json.DefaultJsonProtocol.immSeqFormat

import java.io.IOException
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.language.postfixOps
import scala.util.Success

class WorkspaceServiceSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with TestDriverComponent
    with RawlsTestUtils
    with Eventually
    with MockitoTestUtils
    with RawlsStatsDTestUtils
    with BeforeAndAfterAll
    with TableDrivenPropertyChecks
    with OptionValues {
  import driver.api._

  val workspace: Workspace = Workspace(
    testData.wsName.namespace,
    testData.wsName.name,
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    currentTime(),
    currentTime(),
    "test",
    Map.empty
  )

  val mockServer: RemoteServicesMockServer = RemoteServicesMockServer()

  val leonardoDAO: MockLeonardoDAO = new MockLeonardoDAO()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)
      extends WorkspaceApiService
      with MethodConfigApiService
      with SubmissionApiService
      with MockUserInfoDirectivesWithUser {

    implicit override val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

    val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))

    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(ctx1)
    lazy val methodConfigurationService: MethodConfigurationService = methodConfigurationServiceConstructor(ctx1)
    lazy val submissionsService: SubmissionsService = submissionsServiceConstructor(ctx1)
    lazy val userService: UserService = userServiceConstructor(ctx1)
    val slickDataSource: SlickDataSource = dataSource

    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())
    val gcsDAO = Mockito.spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val googleIamDAO: MockGoogleIamDAO = Mockito.spy(new MockGoogleIamDAO)
    val googleStorageDAO: MockGoogleStorageDAO = Mockito.spy(new MockGoogleStorageDAO)
    val samDAO = Mockito.spy(new MockSamDAO(dataSource))
    val gpsDAO = new org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
    val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    val leonardoService = mock[LeonardoService](RETURNS_SMART_NULLS)
    when(
      leonardoService.cleanupResources(any[GoogleProjectId], any[UUID], any[RawlsRequestContext])(any[ExecutionContext])
    )
      .thenReturn(Future.successful())
    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO()
    val policyService = mock[PolicyService](RETURNS_SMART_NULLS)
    when(policyService.createWorkspacePao(any(), any(), any())).thenReturn(Future.unit)
    when(policyService.mergeWorkspacePao(any(), any(), any())).thenReturn(Future.unit)
    when(policyService.getPao(any(), any())).thenReturn(Future.successful(Option(new TpsPaoGetResult())))
    when(policyService.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)

    val notificationTopic = "test-notification-topic"
    val notificationDAO = Mockito.spy(new PubSubNotificationDAO(gpsDAO, notificationTopic))

    val testConf = ConfigFactory.load()

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      slickDataSource
    )
    val submissionSupervisor = system.actorOf(
      SubmissionSupervisor
        .props(
          executionServiceCluster,
          new UncoordinatedDataSourceAccess(slickDataSource),
          samDAO,
          gcsDAO,
          _ => mock[EntityService],
          mockNotificationDAO,
          SubmissionMonitorConfig(1 second, 30 days, true, 20000, true, true),
          workbenchMetricBaseName = "test"
        )
        .withDispatcher("submission-monitor-dispatcher")
    )

    val servicePerimeterServiceConfig = ServicePerimeterServiceConfig(
      Map(
        ServicePerimeterName("theGreatBarrier") -> Seq(GoogleProjectNumber("555555"), GoogleProjectNumber("121212")),
        ServicePerimeterName("anotherGoodName") -> Seq(GoogleProjectNumber("777777"), GoogleProjectNumber("343434"))
      ),
      1 second,
      5 seconds
    )
    val servicePerimeterService = mock[ServicePerimeterServiceImpl](RETURNS_SMART_NULLS)
    when(servicePerimeterService.overwriteGoogleProjectsInPerimeter(any[ServicePerimeterName], any[DataAccess]))
      .thenReturn(DBIO.successful(()))

    val billingProfileManagerDAO = mock[BillingProfileManagerDAOImpl](RETURNS_SMART_NULLS)

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      samDAO,
      MockBigQueryServiceFactory.ioFactory(),
      testConf.getString("gcs.pathToCredentialJson"),
      servicePerimeterService,
      billingProfileManagerDAO,
      mock[WorkspaceManagerDAO],
      mock[NotificationDAO]
    ) _

    val genomicsServiceConstructor = GenomicsServiceImpl.constructor(
      slickDataSource,
      gcsDAO
    ) _

    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService(
      "fakeTableName",
      "fakeDatePartitionColumn",
      "fakeServiceProject",
      31,
      bigQueryDAO
    )
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-",
      "us-central1"
    )
    val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(testConf)
    override val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService =
      MultiCloudWorkspaceService.constructor(
        dataSource,
        workspaceManagerDAO,
        mock[BillingProfileManagerDAOImpl],
        samDAO,
        multiCloudWorkspaceConfig,
        leonardoDAO,
        workbenchMetricBaseName
      )
    lazy val mcWorkspaceService: MultiCloudWorkspaceService = multiCloudWorkspaceServiceConstructor(ctx1)

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService =
      new RequesterPaysSetupServiceImpl(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val bigQueryServiceFactory: GoogleBigQueryServiceFactoryImpl = MockBigQueryServiceFactory.ioFactory()
    val entityManager = EntityManager.defaultEntityManager(
      dataSource,
      new WorkspaceSettingRepository(dataSource),
      testConf.getBoolean("entityStatisticsCache.enabled"),
      testConf.getDuration("entities.queryTimeout"),
      workbenchMetricBaseName
    )

    val entityServiceConstructor =
      EntityService.constructor(slickDataSource, samDAO, workbenchMetricBaseName = "test", entityManager, 1000) _

    val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
    val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
    val resourceBufferService = Mockito.spy(new ResourceBufferServiceImpl(resourceBufferDAO, resourceBufferConfig))
    val resourceBufferSaEmail = resourceBufferConfig.saEmail

    val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)

    val terraBillingProjectOwnerRole = "fakeTerraBillingProjectOwnerRole"
    val terraWorkspaceCanComputeRole = "fakeTerraWorkspaceCanComputeRole"
    val terraWorkspaceNextflowRole = "fakeTerraWorkspaceNextflowRole"
    val terraBucketReaderRole = "fakeTerraBucketReaderRole"
    val terraBucketWriterRole = "fakeTerraBucketWriterRole"

    val fastPassConfig = FastPassConfig.apply(testConf)
    val fastPassServiceConstructor = FastPassServiceImpl.constructor(
      fastPassConfig,
      googleIamDAO,
      googleStorageDAO,
      gcsDAO,
      samDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole
    ) _

    val workspaceRepository = new WorkspaceRepository(slickDataSource)
    val workspaceSettingRepository = new WorkspaceSettingRepository(slickDataSource)

    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      executionServiceCluster,
      workspaceManagerDAO,
      leonardoService,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      workbenchMetricBaseName,
      workspaceServiceConfig,
      requesterPaysSetupService,
      resourceBufferService,
      servicePerimeterService,
      googleIamDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager,
      fastPassServiceConstructor,
      policyService
    ) _

    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName
    )

    override val methodConfigurationServiceConstructor: RawlsRequestContext => MethodConfigurationService =
      MethodConfigurationService.constructor(
        slickDataSource,
        samDAO,
        methodRepoDAO,
        methodConfigResolver,
        entityManager,
        workspaceRepository,
        workbenchMetricBaseName
      ) _

    override val submissionsServiceConstructor: RawlsRequestContext => SubmissionsService =
      SubmissionsService.constructor(
        slickDataSource,
        entityManager,
        methodRepoDAO,
        new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
        executionServiceCluster,
        methodConfigResolver,
        gcsDAO,
        samDAO,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        workbenchMetricBaseName,
        submissionCostService,
        genomicsServiceConstructor,
        workspaceServiceConfig,
        workspaceRepository,
        workspaceSettingRepository,
        entityServiceConstructor
      ) _

    def cleanupSupervisor =
      submissionSupervisor ! PoisonPill
  }

  class TestApiServiceWithCustomSamDAO(dataSource: SlickDataSource, override val user: RawlsUser)
      extends TestApiService(dataSource, user) {
    override val samDAO: CustomizableMockSamDAO = Mockito.spy(new CustomizableMockSamDAO(dataSource))

    // these need to be overridden to use the new samDAO
    override val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    override val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)
  }

  class TestApiServiceWithMockFastPassService(dataSource: SlickDataSource, override val user: RawlsUser)
      extends TestApiService(dataSource, user) {
    val (mockFastPassService, fastPassMockGcsDAO, fastPassMockSamDAO) =
      MockFastPassService
        .setup(
          user,
          Seq(testData.userOwner, testData.userWriter, testData.userReader),
          fastPassConfig,
          googleIamDAO,
          googleStorageDAO,
          terraBillingProjectOwnerRole,
          terraWorkspaceCanComputeRole,
          terraWorkspaceNextflowRole,
          terraBucketReaderRole,
          terraBucketWriterRole
        )(ctx1, dataSource)

    override val fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPassServiceImpl =
      (_: RawlsRequestContext, _: SlickDataSource) => mockFastPassService
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  def withTestDataServicesCustomSamAndUser[T](user: RawlsUser)(testCode: TestApiServiceWithCustomSamDAO => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServicesCustomSam(dataSource, user)(testCode)
    }

  def withTestDataServicesCustomSam[T](testCode: TestApiServiceWithCustomSamDAO => T): T =
    withTestDataServicesCustomSamAndUser(testData.userOwner)(testCode)

  def withTestDataServicesCustomFastPassAndUser[T](
    user: RawlsUser
  )(testCode: TestApiServiceWithMockFastPassService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServicesCustomFastPass(dataSource, user)(testCode)
    }
  def withTestDataServicesCustomFastPass[T](testCode: TestApiServiceWithMockFastPassService => T): T =
    withTestDataServicesCustomFastPassAndUser(testData.userOwner)(testCode)

  def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  private def withServicesCustomSam[T](dataSource: SlickDataSource, user: RawlsUser)(
    testCode: TestApiServiceWithCustomSamDAO => T
  ) = {
    val apiService = new TestApiServiceWithCustomSamDAO(dataSource, user)

    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  private def withServicesCustomFastPass[T](dataSource: SlickDataSource, user: RawlsUser)(
    testCode: TestApiServiceWithMockFastPassService => T
  ) = {
    val apiService = new TestApiServiceWithMockFastPassService(dataSource, user)

    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  behavior of "WorkspaceService ACL methods"
  it should "retrieve ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val vComplete = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)

    assertResult(
      WorkspaceACL(
        Map(
          testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
          testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false, true),
          testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
        )
      )
    ) {
      vComplete
    }
  }

  private def toRawlsRequestContext(user: RawlsUser) = RawlsRequestContext(
    UserInfo(user.userEmail, OAuth2BearerToken(""), 0, user.userSubjectId)
  )
  private def populateWorkspacePolicies(services: TestApiService, workspace: Workspace = testData.workspace): Unit = {
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toRawlsRequestContext(testData.userOwner))
      _ <- services.samDAO.registerUser(toRawlsRequestContext(testData.userWriter))
      _ <- services.samDAO.registerUser(toRawlsRequestContext(testData.userReader))

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.owner,
        SamPolicy(
          Set(WorkbenchEmail(testData.userOwner.userEmail.value)),
          Set(SamWorkspaceActions.own,
              SamWorkspaceActions.write,
              SamWorkspaceActions.read,
              SamWorkspaceActions.lock,
              SamWorkspaceActions.unlock
          ),
          Set(SamWorkspaceRoles.owner)
        ),
        testContext
      )

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.writer,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)),
                  Set(SamWorkspaceActions.write, SamWorkspaceActions.read),
                  Set(SamWorkspaceRoles.writer)
        ),
        testContext
      )

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.reader,
        SamPolicy(Set(WorkbenchEmail(testData.userReader.userEmail.value)),
                  Set(SamWorkspaceActions.read),
                  Set(SamWorkspaceRoles.reader)
        ),
        testContext
      )

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty),
        testContext
      )
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace,
                                           workspace.workspaceId,
                                           SamWorkspacePolicyNames.shareReader,
                                           SamPolicy(Set.empty, Set.empty, Set.empty),
                                           testContext
      )
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace,
                                           workspace.workspaceId,
                                           SamWorkspacePolicyNames.shareWriter,
                                           SamPolicy(Set.empty, Set.empty, Set.empty),
                                           testContext
      )
      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.canCompute,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)), Set.empty, Set.empty),
        testContext
      )
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace,
                                           workspace.workspaceId,
                                           SamWorkspacePolicyNames.projectOwner,
                                           SamPolicy(Set.empty, Set.empty, Set.empty),
                                           testContext
      )
    } yield ()

    Await.result(populateAcl, Duration.Inf)
  }

  behavior of "checkSamActionWithLock"

  it should "pass sam read action check for a user with read access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(
    testData.userReader
  ) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(
      services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.read),
      Duration.Inf
    )
    assertResult(true) {
      rqComplete
    }
  }

  it should "pass sam read action check for a user with read access in a locked workspace" in
    withTestDataServicesCustomSam { services =>
      populateWorkspacePolicies(services,
                                testData.workspaceNoSubmissions
      ) // can't lock a workspace with running submissions, which the default workspace has
      Await.result(services.workspaceService.lockWorkspace(testData.workspaceNoSubmissions.toWorkspaceName),
                   Duration.Inf
      )

      // generate a new workspace service with a reader user info so we can ask if a reader can access it
      val readerWorkspaceService = services.workspaceServiceConstructor(
        RawlsRequestContext(
          UserInfo(testData.userReader.userEmail, OAuth2BearerToken("token"), 0, testData.userReader.userSubjectId)
        )
      )
      val rqComplete =
        Await.result(readerWorkspaceService.checkSamActionWithLock(testData.workspaceNoSubmissions.toWorkspaceName,
                                                                   SamWorkspaceActions.read
                     ),
                     Duration.Inf
        )
      assertResult(true) {
        rqComplete
      }
    }

  it should "fail sam write action check for a user with read access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(
    testData.userReader
  ) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(
      services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.write),
      Duration.Inf
    )
    assertResult(false) {
      rqComplete
    }
  }

  it should "pass sam write action check for a user with write access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(
    testData.userWriter
  ) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(
      services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.write),
      Duration.Inf
    )
    assertResult(true) {
      rqComplete
    }
  }

  // this is the important test!
  it should "fail sam write action check for a user with write access in a locked workspace" in withTestDataServicesCustomSam {
    services =>
      // first lock the workspace as the owner
      populateWorkspacePolicies(services,
                                testData.workspaceNoSubmissions
      ) // can't lock a workspace with running submissions, which default workspace has
      Await.result(services.workspaceService.lockWorkspace(testData.workspaceNoSubmissions.toWorkspaceName),
                   Duration.Inf
      )

      // now as a writer, ask if we can write it. but it's locked!
      val readerWorkspaceService = services.workspaceServiceConstructor(
        RawlsRequestContext(
          UserInfo(testData.userWriter.userEmail, OAuth2BearerToken("token"), 0, testData.userWriter.userSubjectId)
        )
      )
      val rqComplete =
        Await.result(readerWorkspaceService.checkSamActionWithLock(testData.workspaceNoSubmissions.toWorkspaceName,
                                                                   SamWorkspaceActions.write
                     ),
                     Duration.Inf
        )
      assertResult(false) {
        rqComplete
      }
  }

  behavior of "WorkspaceService workspace locking and unlocking"

  it should "lock a workspace with terminated submissions" in withTestDataServices { services =>
    // check workspace is not locked
    assert(!testData.workspaceTerminatedSubmissions.isLocked)

    val rqComplete =
      Await.result(services.workspaceService.lockWorkspace(testData.workspaceTerminatedSubmissions.toWorkspaceName),
                   Duration.Inf
      )

    assertResult(true) {
      rqComplete
    }

    val rqCompleteAgain =
      Await.result(services.workspaceService.lockWorkspace(testData.workspaceTerminatedSubmissions.toWorkspaceName),
                   Duration.Inf
      )

    assertResult(false) {
      rqCompleteAgain
    }

    // check workspace is locked
    assert {
      runAndWait(workspaceQuery.findByName(testData.workspaceTerminatedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "fail to lock a workspace with active submissions" in withTestDataServices { services =>
    // check workspace is not locked
    assert(!testData.workspaceMixedSubmissions.isLocked)

    val except: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        services.workspaceService.lockWorkspace(
          WorkspaceName(testData.workspaceMixedSubmissions.namespace, testData.workspaceMixedSubmissions.name)
        ),
        Duration.Inf
      )
    }

    assertResult(StatusCodes.Conflict) {
      except.errorReport.statusCode.get
    }

    assert {
      !runAndWait(workspaceQuery.findByName(testData.workspaceMixedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "fail to unlock a migrating workspace" in withTestDataServices { services =>
    runAndWait(
      for {
        _ <- slickDataSource.dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(
          testData.workspace,
          Option("US")
        )
        attempts <- slickDataSource.dataAccess.multiregionalBucketMigrationQuery.getMigrationAttempts(
          testData.workspace
        )
        _ <- slickDataSource.dataAccess.multiregionalBucketMigrationQuery.update(
          attempts.head.id,
          slickDataSource.dataAccess.multiregionalBucketMigrationQuery.startedCol,
          Some(Timestamp.from(Instant.now))
        )
      } yield (),
      Duration.Inf
    )

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.unlockWorkspace(testData.workspace.toWorkspaceName), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "deleteWorkspace"

  it should "delete a workspace with linked bond service account" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    // add a bond sa link
    Await.result(
      services.requesterPaysSetupService.grantRequesterPaysToLinkedSAs(userInfo, testData.workspaceNoSubmissions),
      Duration.Inf
    )

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    // check that the workspace has been deleted
    runAndWait(workspaceQuery.findByName(testData.wsName3)) shouldBe None

  }

  it should "delete a workspace with no submissions" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    // check that the workspace has been deleted
    runAndWait(workspaceQuery.findByName(testData.wsName3)) shouldBe None
  }

  it should "delete a workspace with succeeded submission" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSuccessfulSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    // Check method configs to be deleted exist
    assertResult(
      Vector(
        MethodConfigurationShort("testConfig2", Some("Sample"), AgoraMethod("myNamespace", "method-a", 1), "dsde"),
        MethodConfigurationShort("testConfig1", Some("Sample"), AgoraMethod("ns-config", "meth1", 1), "ns")
      )
    ) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSuccessfulSubmission))
    }

    // Check if submissions on workspace exist
    assertResult(List(testData.submissionSuccessful1)) {
      runAndWait(submissionQuery.list(testData.workspaceSuccessfulSubmission))
    }

    // Check if entities on workspace exist
    assertResult(20) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId))
          .length
          .result
      )
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName4), Duration.Inf)

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    // check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSuccessfulSubmission))
    }

    // Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceSuccessfulSubmission))
    }

    // Check if entities on workspace have been deleted
    assertResult(0) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId))
          .length
          .result
      )
    }
  }

  it should "delete a workspace with failed submission" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceFailedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    // Check method configs to be deleted exist
    assertResult(
      Vector(MethodConfigurationShort("testConfig1", Some("Sample"), AgoraMethod("ns-config", "meth1", 1), "ns"))
    ) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceFailedSubmission))
    }

    // Check if submissions on workspace exist
    assertResult(List(testData.submissionFailed)) {
      runAndWait(submissionQuery.list(testData.workspaceFailedSubmission))
    }

    // Check if entities on workspace exist
    assertResult(20) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId))
          .length
          .result
      )
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName5), Duration.Inf)

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    // check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceFailedSubmission))
    }

    // Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceFailedSubmission))
    }

    // Check if entities on workspace exist
    assertResult(0) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId))
          .length
          .result
      )
    }
  }

  it should "delete a workspace with submitted submission" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSubmittedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    // Check method configs to be deleted exist
    assertResult(
      Vector(MethodConfigurationShort("testConfig1", Some("Sample"), AgoraMethod("ns-config", "meth1", 1), "ns"))
    ) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSubmittedSubmission))
    }

    // Check if submissions on workspace exist
    assertResult(List(testData.submissionSubmitted)) {
      runAndWait(submissionQuery.list(testData.workspaceSubmittedSubmission))
    }

    // Check if entities on workspace exist
    assertResult(20) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId))
          .length
          .result
      )
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName6), Duration.Inf)

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    // check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSubmittedSubmission))
    }

    // Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceSubmittedSubmission))
    }

    // Check if entities on workspace exist
    assertResult(0) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId))
          .length
          .result
      )
    }
  }

  it should "delete a workspace with mixed submissions" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceMixedSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    // Check method configs to be deleted exist
    assertResult(
      Vector(MethodConfigurationShort("testConfig1", Some("Sample"), AgoraMethod("ns-config", "meth1", 1), "ns"))
    ) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceMixedSubmissions))
    }

    // Check if submissions on workspace exist
    assertResult(2) {
      runAndWait(submissionQuery.list(testData.workspaceMixedSubmissions)).length
    }

    // Check if entities on workspace exist
    assertResult(20) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId))
          .length
          .result
      )
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName7), Duration.Inf)

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    // check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceMixedSubmissions))
    }

    // Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceMixedSubmissions))
    }

    // Check if entities on workspace exist
    assertResult(0) {
      runAndWait(
        entityQuery
          .findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId))
          .length
          .result
      )
    }

  }

  it should "handle 404s from Sam when deleting a workspace" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    when(
      services.samDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                     ArgumentMatchers.eq(testData.workspaceNoSubmissions.workspaceId),
                                     any[RawlsRequestContext]
      )
    ).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "404 from Sam"))))

    when(
      services.samDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
                                     any[String],
                                     any[RawlsRequestContext]
      )
    ).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "404 from Sam"))))

    // delete the workspace and verify it has been deleted
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }
  }

  it should "fail if Sam throws a 403 in delete workspace" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    when(
      services.samDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                     ArgumentMatchers.eq(testData.workspaceNoSubmissions.workspaceId),
                                     any[RawlsRequestContext]
      )
    ).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "403 from Sam"))))

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)
    }
    assertResult(Some(StatusCodes.Forbidden)) {
      error.errorReport.statusCode
    }
  }

  it should "fail if Sam throws a 500 in delete workflowCollection" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    when(
      services.samDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
                                     any[String],
                                     any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "500 from Sam")))
    )

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)
    }
    assertResult(Some(StatusCodes.InternalServerError)) {
      error.errorReport.statusCode
    }
  }

  it should "fail if called with an MC workspace" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    val workspace = Await.result(
      services.workspaceService.createWorkspace(workspaceRequest),
      Duration.Inf
    )
    // hack to set this workspace to be an MC workspace, since there is no real way
    // to create an MC workspace anymore
    Await.result(
      services.slickDataSource.inTransaction { _ =>
        sql"""update WORKSPACE set workspace_type = 'mc' where name = $workspaceName""".asUpdate
      },
      Duration.Inf
    )
    assertResult(Option(workspace.toWorkspaceName)) {
      runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name))).map(_.toWorkspaceName)
    }

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.deleteWorkspace(
                     WorkspaceName(workspace.namespace, workspace.name)
                   ),
                   Duration.Inf
      )
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)

  }

  it should "delete a workspace when PAO exists or has exceptions" in withTestDataServices { services =>
    // Check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    // Mock the PAO deletion [ignores any exceptions]
    when(services.policyService.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)

    // Delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    // Verify that the PAO deletion was called
    verify(services.policyService)
      .deleteWorkspacePao(ArgumentMatchers.eq(testData.workspaceNoSubmissions.workspaceIdAsUUID), any())

    // Check that the workspace has been deleted
    runAndWait(workspaceQuery.findByName(testData.wsName3)) shouldBe None
  }

  it should "fail if Sam reports children for this workspace" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    when(
      services.samDAO.listResourceChildren(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                           ArgumentMatchers.eq(testData.workspaceNoSubmissions.workspaceId),
                                           any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(
        Seq(SamFullyQualifiedResourceId("fake-id", "notebook-cluster"))
      ) // Simulate that there are children resources
    )

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)
    }
    assertResult(Some(StatusCodes.BadRequest)) {
      error.errorReport.statusCode
    }
  }

  behavior of "getTags"

  it should "return the correct tags from autocomplete" in withTestDataServices { services =>
    // when no tags, return empty set
    val res1 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
    assertResult(Vector.empty[WorkspaceTag]) {
      res1
    }

    // add some tags
    Await.result(
      services.workspaceService.updateWorkspace(
        testData.wsName,
        Seq(AddListMember(AttributeName.withTagsNS(), AttributeString("cancer")),
            AddListMember(AttributeName.withTagsNS(), AttributeString("cantaloupe"))
        )
      ),
      Duration.Inf
    )

    Await.result(
      services.workspaceService.updateWorkspace(
        testData.wsName7,
        Seq(AddListMember(AttributeName.withTagsNS(), AttributeString("cantaloupe")),
            AddListMember(AttributeName.withTagsNS(), AttributeString("buffalo"))
        )
      ),
      Duration.Inf
    )

    // searching for tag that doesn't exist should return empty set
    val res2 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
    assertResult(Vector.empty[String]) {
      res2
    }

    // searching for tag that does exist should return the tag (query string case doesn't matter)
    val res3 = Await.result(services.workspaceService.getTags(Some("bUf")), Duration.Inf)
    assertResult(Vector(WorkspaceTag("buffalo", 1))) {
      res3
    }

    val res4 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
    assertResult(Vector(WorkspaceTag("cancer", 1))) {
      res4
    }

    // searching for multiple tag that does exist should return the tags (query string case doesn't matter)
    // should be sorted by counts of tags
    val res5 = Await.result(services.workspaceService.getTags(Some("cAn")), Duration.Inf)
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("cancer", 1))) {
      res5
    }

    // searching for with no query should return all tags
    val res6 = Await.result(services.workspaceService.getTags(None), Duration.Inf)
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("buffalo", 1), WorkspaceTag("cancer", 1))) {
      res6
    }

    // setting a limit should limit the number of tags returned
    val res7 = Await.result(services.workspaceService.getTags(None, Some(2)), Duration.Inf)
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("buffalo", 1))) {
      res7
    }

    // remove tags
    Await.result(
      services.workspaceService.updateWorkspace(testData.wsName, Seq(RemoveAttribute(AttributeName.withTagsNS()))),
      Duration.Inf
    )
    Await.result(
      services.workspaceService.updateWorkspace(testData.wsName7, Seq(RemoveAttribute(AttributeName.withTagsNS()))),
      Duration.Inf
    )

    // make sure that tags no longer exists
    val res8 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
    assertResult(Vector.empty[WorkspaceTag]) {
      res8
    }

  }

  behavior of "maybeShareProjectComputePolicy"

  for (
    (policyName, shouldShare) <- Seq((SamWorkspacePolicyNames.writer, false),
                                     (SamWorkspacePolicyNames.canCompute, true),
                                     (SamWorkspacePolicyNames.reader, false)
    )
  )
    it should s"${if (!shouldShare) "not " else ""}share billing compute when workspace $policyName access granted" in withTestDataServicesCustomSam {
      services =>
        val email = s"${UUID.randomUUID}@bar.com"
        val results = Set((policyName, email))
        Await.result(
          services.workspaceService.maybeShareProjectComputePolicy(results, testData.workspace.toWorkspaceName),
          Duration.Inf
        )

        val expectedPolicyEntry = (SamResourceTypeNames.billingProject,
                                   testData.workspace.namespace,
                                   SamBillingProjectPolicyNames.canComputeUser,
                                   email
        )
        if (shouldShare) {
          services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set(expectedPolicyEntry)
        } else {
          services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
        }
    }

  behavior of "RequesterPays"

  it should "return Unit when adding linked service accounts to workspace" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { _ =>
      val rqComplete: Unit =
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      assertResult(()) {
        rqComplete
      }
    }
  }

  it should "return a 404 ErrorReport when adding linked service accounts to workspace which does not exist" in withTestDataServices {
    services =>
      withWorkspaceContext(testData.workspace) { _ =>
        val error = intercept[RawlsExceptionWithErrorReport] {
          Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(
                         testData.workspace.toWorkspaceName.copy(name = "DNE")
                       ),
                       Duration.Inf
          )
        }
        assertResult(Some(StatusCodes.NotFound)) {
          error.errorReport.statusCode
        }
      }
  }

  it should "return a 404 ErrorReport when adding linked service accounts to workspace with no access" in withTestDataServicesCustomSamAndUser(
    RawlsUser(RawlsUserSubjectId("no-access"), RawlsUserEmail("no-access"))
  ) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { _ =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      }
      assertResult(Some(StatusCodes.NotFound)) {
        error.errorReport.statusCode
      }
    }
  }

  it should "return a 403 Error Report when adding add linked service accounts to workspace with read access" in withTestDataServicesCustomSamAndUser(
    testData.userReader
  ) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { _ =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      }
      assertResult(Some(StatusCodes.Forbidden)) {
        error.errorReport.statusCode
      }
    }
  }

  it should "return Unit when removing linked service accounts from workspace" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { _ =>
      val rqComplete: Unit =
        Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      assertResult(()) {
        rqComplete
      }
    }
  }

  it should "return Unit when removing linked service accounts from workspace which does not exist" in withTestDataServices {
    services =>
      withWorkspaceContext(testData.workspace) { _ =>
        val rqComplete: Unit = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(
                                              testData.workspace.toWorkspaceName.copy(name = "DNE")
                                            ),
                                            Duration.Inf
        )
        assertResult(()) {
          rqComplete
        }
      }
  }

  it should "return Unit when removing linked service accounts from workspace with no access" in withTestDataServicesCustomSamAndUser(
    RawlsUser(RawlsUserSubjectId("no-access"), RawlsUserEmail("no-access"))
  ) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { _ =>
      val rqComplete: Unit =
        Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      assertResult(()) {
        rqComplete
      }
    }
  }

  it should "return Unit when removing linked service accounts from workspace with read access" in withTestDataServicesCustomSamAndUser(
    testData.userReader
  ) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { _ =>
      val rqComplete: Unit =
        Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      assertResult(()) {
        rqComplete
      }
    }
  }

  "createWorkspace" should "create a V2 Workspace" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.googleProjectId.value should not be empty
    workspace.googleProjectNumber should not be empty
  }

  it should "create desired policies on the Sam workspace resource" in withTestDataServices { services =>
    val newWorkspaceName = "workspaceResourcePoliciesTest"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val requestedPoliciesCaptor = captor[Map[SamResourcePolicyName, SamPolicy]]
    verify(services.samDAO).createResourceFull(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspace.workspaceId),
      requestedPoliciesCaptor.capture,
      any[Set[String]],
      any[RawlsRequestContext],
      ArgumentMatchers.eq(None)
    )

    val requestedPolicies = requestedPoliciesCaptor.getValue
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.projectOwner, fail("Missing project-owner policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.projectOwner, SamWorkspaceRoles.owner)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.owner, fail("Missing owner policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.owner)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.writer, fail("Missing writer policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.writer)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.reader, fail("Missing reader policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.reader)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.shareWriter, fail("Missing share-writer policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.shareWriter)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.shareReader, fail("Missing share-reader policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.shareReader)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.canCompute, fail("Missing can-compute policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.canCompute)
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.canCatalog, fail("Missing can-catalog policy"))
      .roles should contain theSameElementsAs Set(SamWorkspaceRoles.canCatalog)
  }

  it should "add user policies if included" in withTestDataServices { services =>
    val newWorkspaceName = "workspaceResourcePoliciesTest"
    val readerOnly = WorkspaceACLUpdate("readerOnly@email.com", WorkspaceAccessLevels.Read, Some(false), Some(false))
    val readerShare = WorkspaceACLUpdate("readerShare@email.com", WorkspaceAccessLevels.Read, Some(true), Some(false))
    val writerOnly = WorkspaceACLUpdate("writerOnly@email.com", WorkspaceAccessLevels.Write, Some(false), Some(false))
    val writerShare = WorkspaceACLUpdate("writerShare@email.com", WorkspaceAccessLevels.Write, Some(true), Some(false))
    val writerCompute =
      WorkspaceACLUpdate("writerCompute@email.com", WorkspaceAccessLevels.Write, Some(true), Some(true))
    val owner = WorkspaceACLUpdate("owner@email.com", WorkspaceAccessLevels.Owner, Some(true), Some(true))
    val workspaceRequest =
      WorkspaceRequest(testData.testProject1Name.value,
                       newWorkspaceName,
                       Map.empty,
                       addUsers = Some(List(readerOnly, readerShare, writerOnly, writerShare, writerCompute, owner))
      )
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val requestedPoliciesCaptor = captor[Map[SamResourcePolicyName, SamPolicy]]
    verify(services.samDAO).createResourceFull(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspace.workspaceId),
      requestedPoliciesCaptor.capture,
      any[Set[String]],
      any[RawlsRequestContext],
      ArgumentMatchers.eq(None)
    )

    val requestedPolicies = requestedPoliciesCaptor.getValue
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.projectOwner, fail("Missing project-owner policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("foo@bar.com"))
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.owner, fail("Missing owner policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("owner-access"),
                                                         WorkbenchEmail("owner@email.com")
    )
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.writer, fail("Missing writer policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("writerOnly@email.com"),
                                                         WorkbenchEmail("writerShare@email.com"),
                                                         WorkbenchEmail("writerCompute@email.com")
    )
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.reader, fail("Missing reader policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("readerOnly@email.com"),
                                                         WorkbenchEmail("readerShare@email.com")
    )
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.shareWriter, fail("Missing share-writer policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("writerShare@email.com"),
                                                         WorkbenchEmail("writerCompute@email.com")
    )
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.shareReader, fail("Missing share-reader policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("readerShare@email.com"))
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.canCompute, fail("Missing can-compute policy"))
      .memberEmails should contain theSameElementsAs Set(WorkbenchEmail("writerCompute@email.com"),
                                                         WorkbenchEmail("owner@email.com")
    )
    requestedPolicies
      .getOrElse(SamWorkspacePolicyNames.canCatalog, fail("Missing can-catalog policy"))
      .memberEmails should contain theSameElementsAs Set.empty
  }

  it should "create Sam resource for google project" in withTestDataServices { services =>
    val newWorkspaceName = "new-workspace"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    // Verify that samDAO.createResourceFull was called
    verify(services.samDAO).createResourceFull(
      ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
      ArgumentMatchers.eq(workspace.googleProjectId.value),
      ArgumentMatchers.eq(Map.empty),
      ArgumentMatchers.eq(Set.empty),
      any[RawlsRequestContext],
      ArgumentMatchers.eq(
        Option(SamFullyQualifiedResourceId(workspace.workspaceId, SamResourceTypeNames.workspace.value))
      )
    )
  }

  it should "fail with 400 if policies are provided for a GCP workspace" in withTestDataServices { services =>
    val error = intercept[RawlsExceptionWithErrorReport] {
      val workspaceName = WorkspaceName(testData.testProject1Name.value, s"${UUID.randomUUID()}")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace,
                                              workspaceName.name,
                                              Map.empty,
                                              policies = Some(List(WorkspacePolicy("fake", "fake", List.empty)))
      )
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  // TODO: This test will need to be deleted when implementing https://broadworkbench.atlassian.net/browse/CA-947
  it should "fail with 400 when the BillingProject is not Ready" in withTestDataServices { services =>
    (CreationStatuses.all - CreationStatuses.Ready).foreach { projectStatus =>
      // Update the BillingProject with the CreationStatus under test
      runAndWait(
        slickDataSource.dataAccess.rawlsBillingProjectQuery.updateCreationStatus(testData.testProject1.projectName,
                                                                                 projectStatus
        )
      )

      // Create a Workspace in the BillingProject
      val error = intercept[RawlsExceptionWithErrorReport] {
        val workspaceName = WorkspaceName(testData.testProject1Name.value, s"ws_with_status_$projectStatus")
        val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }

      error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    }
  }

  it should "fail with 400 if specified Namespace/Billing Project does not exist" in withTestDataServices { services =>
    val workspaceRequest = WorkspaceRequest("nonexistent_namespace", "kermits_pond", Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail with 400 if Billing Project does not have a Billing Account specified" in withTestDataServices {
    services =>
      // Update BillingProject to wipe BillingAccount field.  Reload BillingProject and confirm that field is empty
      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingAccount(
            testData.testProject1.projectName,
            billingAccount = None,
            testData.userOwner.userSubjectId
          )
          updatedBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name)
        } yield updatedBillingProject.value.billingAccount shouldBe empty
      }

      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "banana_palooza", Map.empty)
      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }
      error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail with 403 and set the invalidBillingAcct field if Rawls does not have the required IAM permissions on the Google Billing Account" in withTestDataServices {
    services =>
      // Preconditions: setup the BillingProject to have the BillingAccountName that will "fail" the permissions check in
      // the MockGoogleServicesDAO.  Then confirm that the BillingProject.invalidBillingAccount field starts as FALSE

      val billingAccountName = services.gcsDAO.inaccessibleBillingAccountName
      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingAccount(
            testData.testProject1.projectName,
            billingAccountName.some,
            testData.userOwner.userSubjectId
          )
          updatedBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name)
        } yield {
          updatedBillingProject.value.billingAccount shouldBe defined
          updatedBillingProject.value.invalidBillingAccount shouldBe false
        }
      }

      // Make the call to createWorkspace and make sure it throws an exception with the correct StatusCode
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "whatever", Map.empty)
      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }
      error.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

      // Make sure that the BillingProject.invalidBillingAccount field was properly updated while attempting to create the
      // Workspace
      val persistedBillingProject =
        runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
      persistedBillingProject.value.invalidBillingAccount shouldBe true
  }

  it should "fail with 502 if Rawls is unable to retrieve the Google Project Number from Google for Workspace's Google Project" in withTestDataServices {
    services =>
      when(services.gcsDAO.getGoogleProject(any[GoogleProjectId]))
        .thenReturn(Future.successful(new Project().setProjectNumber(null)))

      val workspaceName = WorkspaceName(testData.testProject1Name.value, "whatever")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }

      error.errorReport.statusCode shouldBe Some(StatusCodes.BadGateway)

      val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
      maybeWorkspace shouldBe None
  }

  it should "set the Billing Account on the Workspace's Google Project to match the Billing Project's Billing Account" in withTestDataServices {
    services =>
      val billingProject = testData.testProject1
      val workspaceName = WorkspaceName(billingProject.projectName.value, "cool_workspace")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      // Project ID gets allocated when creating the Workspace, so we don't care what it is here.  We do care that
      // whatever that Google Project is, we set the right Billing Account on it, which is the Billing Account specified
      // in the Billing Project.  Additionally, only when creating a new Workspace, we can `force` the update (and ignore
      // the "oldBillingAccount" value
      verify(services.gcsDAO).setBillingAccountName(
        any[GoogleProjectId],
        ArgumentMatchers.eq(billingProject.billingAccount.get),
        any[RawlsTracingContext]
      )
  }

  it should "fail to create a database object when GoogleServicesDAO throws an exception when updating billing account" in withTestDataServices {
    services =>
      doReturn(Future.failed(new Exception("failed")), null)
        .when(services.gcsDAO)
        .setBillingAccountName(
          ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
          ArgumentMatchers.eq(RawlsBillingAccountName("fakeBillingAcct")),
          any[RawlsTracingContext]
        )

      val workspaceName = WorkspaceName(testData.testProject1Name.value, "sad_workspace")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

      intercept[Exception] {
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }

      val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
      maybeWorkspace shouldBe None
  }

  it should "not try to modify the Service Perimeter if the Billing Project does not specify a Service Perimeter" in withTestDataServices {
    services =>
      val newWorkspaceName = "space_for_workin"
      val billingProject = testData.testProject1

      // Pre-condition: make sure that the Billing Project we're adding the Workspace to DOES NOT specify a Service
      // Perimeter
      billingProject.servicePerimeter shouldBe empty

      val workspaceRequest = WorkspaceRequest(billingProject.projectName.value, newWorkspaceName, Map.empty)
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      // Verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was NOT called
      verify(services.googleAccessContextManagerDAO, Mockito.never())
        .overwriteProjectsInServicePerimeter(any[ServicePerimeterName], any[Set[String]])
  }

  it should "claim a Google Project from Resource Buffering Service" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    verify(services.resourceBufferService).getGoogleProjectFromBuffer(any[ProjectPoolType], any[String])
  }

  it should "Update a Google Project name after claiming a project from Resource Buffering Service" in withTestDataServices {
    services =>
      val newWorkspaceNamespace = "short_-NS1"
      val newWorkspaceName =
        "plus Long_ name to get past 30 chars since the google-project name is truncated at 30 chars and formatted as namespace--name"
      val billingProject = RawlsBillingProject(UUID.randomUUID(),
                                               RawlsBillingProjectName(newWorkspaceNamespace),
                                               CreationStatuses.Ready,
                                               Option(RawlsBillingAccountName("fakeBillingAcct")),
                                               None
      )
      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      val workspaceRequest = WorkspaceRequest(newWorkspaceNamespace, newWorkspaceName, Map.empty)
      val captor = ArgumentCaptor.forClass(classOf[Project])

      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      verify(services.gcsDAO).updateGoogleProject(ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
                                                  captor.capture()
      )
      val capturedProject =
        captor.getValue
          .asInstanceOf[Project] // Explicit cast needed since Scala type interference and capturing parameters with Mockito don't play nicely together here

      val expectedProjectName = "plus Long- name to get past 30"
      val actualProjectName = capturedProject.getName
      actualProjectName shouldBe expectedProjectName
  }

  it should "Apply labels to a Google Project after claiming a project from Resource Buffering Service" in withTestDataServices {
    services =>
      val newWorkspaceNamespace = "Long_Namespace---30-char-limit"
      val newWorkspaceName = "Plus Long_ name to get past 63 chars since the labels are truncated at 63 chars"
      val billingProject = RawlsBillingProject(UUID.randomUUID(),
                                               RawlsBillingProjectName(newWorkspaceNamespace),
                                               CreationStatuses.Ready,
                                               Option(RawlsBillingAccountName("fakeBillingAcct")),
                                               None
      )
      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      val workspaceRequest = WorkspaceRequest(newWorkspaceNamespace, newWorkspaceName, Map.empty)
      val captor = ArgumentCaptor.forClass(classOf[Project])

      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      verify(services.gcsDAO).updateGoogleProject(ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
                                                  captor.capture()
      )
      val capturedProject =
        captor.getValue
          .asInstanceOf[Project] // Explicit cast needed since Scala type interference and capturing parameters with Mockito don't play nicely together here

      val expectedNewLabels = Map(
        "workspacenamespace" -> "long_namespace---30-char-limit",
        "workspacename" -> "plus-long_-name-to-get-past-63-chars-since-the-labels-are-trunc",
        "workspaceid" -> workspace.workspaceId
      )
      val numberOfLabelsFromBuffer = 3
      val expectedLabelSize = numberOfLabelsFromBuffer + expectedNewLabels.size
      val actualLabels = capturedProject.getLabels.asScala

      actualLabels.size shouldBe expectedLabelSize
      actualLabels should contain allElementsOf expectedNewLabels
  }

  it should "create a workspace bucket with secure logging if told to, even without an auth domain" in withTestDataServices {
    services =>
      val newWorkspaceName = "secure_space_for_workin"
      val workspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        newWorkspaceName,
        Map.empty,
        authorizationDomain = None,
        enhancedBucketLogging = Some(true)
      )

      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "create a workspace bucket with secure logging if an auth domain is specified" in withTestDataServices {
    services =>
      val newWorkspaceName = "secure_space_for_workin"
      val workspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        newWorkspaceName,
        Map.empty,
        authorizationDomain = Option(Set(testData.dbGapAuthorizedUsersGroup))
      )

      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "create a new PAO for the workspace" in withTestDataServices { services =>
    val workspaceRequest = WorkspaceRequest(testData.workspace.namespace, "paoWs", Map.empty)

    val newWs = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    verify(services.policyService).createWorkspacePao(ArgumentMatchers.eq(UUID.fromString(newWs.workspaceId)),
                                                      any(),
                                                      any()
    )
    verify(services.policyService, never()).mergeWorkspacePao(any(), any(), any())
  }

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "creating a Workspace in a Service Perimeter" should "attempt to overwrite the correct Service Perimeter" in withTestDataServices {
    services =>
      // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
      val servicePerimeterName: ServicePerimeterName =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters.keys.head
      val billingProject1 = testData.testProject1
      val billingProject2 = testData.testProject2
      val billingProjects = Seq(billingProject1, billingProject2)
      val workspacesPerProject = 2

      // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
      // the Billing Projects and therefore in the Perimeter
      billingProjects.foreach { bp =>
        runAndWait {
          for {
            _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateServicePerimeter(bp.projectName,
                                                                                            servicePerimeterName.some
            )
            updatedBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName)
          } yield updatedBillingProject.value.servicePerimeter.value shouldBe servicePerimeterName
        }

        (1 to workspacesPerProject).map { n =>
          val workspace = testData.workspace.copy(
            namespace = bp.projectName.value,
            name = s"${bp.projectName.value}Workspace$n",
            workspaceId = UUID.randomUUID().toString,
            googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
          )
          runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
        }
      }

      // Test setup is done, now we're getting to the test
      // Make a call to Create a new Workspace in the same Billing Project
      val workspaceName = WorkspaceName(testData.testProject1Name.value, "cool_workspace")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      val servicePerimeterNameCaptor = captor[ServicePerimeterName]
      // verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was called exactly once and capture
      // the arguments passed to it so that we can verify that they were correct
      verify(services.servicePerimeterService).overwriteGoogleProjectsInPerimeter(servicePerimeterNameCaptor.capture,
                                                                                  any[DataAccess]
      )
      servicePerimeterNameCaptor.getValue shouldBe servicePerimeterName

      // verify that we set the folder for the perimeter
      verify(services.gcsDAO).addProjectToFolder(ArgumentMatchers.eq(workspace.googleProjectId), any[String])
  }

  "cloneWorkspace" should "create a Workspace" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace =
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.googleProjectNumber should not be empty
    workspace.workspaceType shouldBe WorkspaceType.RawlsWorkspace
    workspace.attributes shouldBe baseWorkspace.attributes
  }

  it should "copy files from the source to the destination asynchronously" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val copyFilesWithPrefix = "copy_me"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value,
                                            newWorkspaceName,
                                            Map.empty,
                                            copyFilesWithPrefix = Option(copyFilesWithPrefix)
    )

    val workspace =
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

    eventually(timeout = timeout(Span(10, Seconds))) {
      runAndWait(slickDataSource.dataAccess.cloneWorkspaceFileTransferQuery.listPendingTransfers())
        .map(_.destWorkspaceId)
        .contains(workspace.workspaceIdAsUUID) shouldBe true
    }
    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.workspaceType shouldBe WorkspaceType.RawlsWorkspace
    workspace.googleProjectNumber should not be empty
  }

  it should "merge destination attributes with source attributes" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    testData.workspace.attributes.get(AttributeName.withDefaultNS("string")).value should be(
      AttributeString("yep, it's a string")
    )
    val newAttributes = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("destination string")
    )
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, newAttributes)

    val workspace =
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.googleProjectNumber should not be empty
    workspace.workspaceType shouldBe WorkspaceType.RawlsWorkspace
    val mergedAttributes = workspace.attributes
    // overrides value in source attributes
    mergedAttributes.get(AttributeName.withDefaultNS("string")).value should be(AttributeString("destination string"))
    // from source attributes
    mergedAttributes.get(AttributeName.withDefaultNS("number")).value should be(AttributeNumber(10))
  }

  it should "create a new PAO and merge the source workspace's PAO into the destination workspace's PAO" in withTestDataServices {
    services =>
      val baseWorkspace = testData.workspace
      val workspaceRequest = WorkspaceRequest(baseWorkspace.namespace, "clone", Map.empty)

      val newlyClonedWs =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      verify(services.policyService).createWorkspacePao(ArgumentMatchers.eq(UUID.fromString(newlyClonedWs.workspaceId)),
                                                        any(),
                                                        any()
      )
      verify(services.policyService).mergeWorkspacePao(ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
                                                       ArgumentMatchers.eq(UUID.fromString(newlyClonedWs.workspaceId)),
                                                       any()
      )
  }

  it should "still clone the workspace if the source workspace doesn't have a PAO" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest(baseWorkspace.namespace, "clone", Map.empty)
    when(services.policyService.getPao(any(), any())).thenReturn(Future(None))

    val newlyClonedWs =
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

    verify(services.policyService).createWorkspacePao(ArgumentMatchers.eq(UUID.fromString(newlyClonedWs.workspaceId)),
                                                      any(),
                                                      any()
    )
    verify(services.policyService, never).mergeWorkspacePao(
      ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
      ArgumentMatchers.eq(UUID.fromString(newlyClonedWs.workspaceId)),
      any()
    )
  }

  it should "fail with 400 if specified Namespace/Billing Project does not exist" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest("nonexistent_namespace", "kermits_pond", Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  // TODO: This test will need to be deleted when implementing https://broadworkbench.atlassian.net/browse/CA-947
  it should "fail with 400 when the BillingProject is not Ready" in withTestDataServices { services =>
    (CreationStatuses.all - CreationStatuses.Ready).foreach { projectStatus =>
      // Update the BillingProject with the CreationStatus under test
      val sourceWorkspace = runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateCreationStatus(
            testData.testProject1.projectName,
            projectStatus
          )
          workspace <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(testData.workspace)
        } yield workspace
      }

      // Create a Workspace in the BillingProject
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.workspaceService.cloneWorkspace(
            sourceWorkspace.toWorkspaceName,
            WorkspaceRequest(namespace = testData.testProject1.projectName.value,
                             name = s"ws_with_status_$projectStatus",
                             Map.empty
            )
          ),
          Duration.Inf
        )
      }

      error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    }
  }

  it should "fail with 400 if Billing Project does not have a Billing Account specified" in withTestDataServices {
    services =>
      // Update BillingProject to wipe BillingAccount field.  Reload BillingProject and confirm that field is empty
      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingAccount(
            testData.testProject1.projectName,
            billingAccount = None,
            testData.userOwner.userSubjectId
          )
          updatedBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name)
        } yield updatedBillingProject.value.billingAccount shouldBe empty
      }

      val baseWorkspace = testData.workspace
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "banana_palooza", Map.empty)
      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )
      }
      error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail with 403 and set the invalidBillingAcct field if Rawls does not have the required IAM permissions on the Google Billing Account" in withTestDataServices {
    services =>
      // Preconditions: setup the BillingProject to have the BillingAccountName that will "fail" the permissions check in
      // the MockGoogleServicesDAO.  Then confirm that the BillingProject.invalidBillingAccount field starts as FALSE
      val billingAccountName = services.gcsDAO.inaccessibleBillingAccountName
      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingAccount(
            testData.testProject1.projectName,
            billingAccountName.some,
            testData.userOwner.userSubjectId
          )
          originalBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name)
        } yield originalBillingProject.value.invalidBillingAccount shouldBe false
      }

      // Make the call to createWorkspace and make sure it throws an exception with the correct StatusCode
      val baseWorkspace = testData.workspace
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "whatever", Map.empty)
      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )
      }
      error.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

      // Make sure that the BillingProject.invalidBillingAccount field was properly updated while attempting to create the
      // Workspace
      val persistedBillingProject =
        runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
      persistedBillingProject.value.invalidBillingAccount shouldBe true
  }

  it should "fail with 502 if Rawls is unable to retrieve the Google Project Number from Google for Workspace's Google Project" in withTestDataServices {
    services =>
      when(services.gcsDAO.getGoogleProject(any[GoogleProjectId]))
        .thenReturn(Future.successful(new Project().setProjectNumber(null)))

      val workspaceName = WorkspaceName(testData.testProject1Name.value, "whatever")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

      val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      }

      error.errorReport.statusCode shouldBe Some(StatusCodes.BadGateway)

      val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
      maybeWorkspace shouldBe None
  }

  it should "set the Billing Account on the Workspace's Google Project to match the Billing Project's Billing Account" in withTestDataServices {
    services =>
      val destBillingProject = testData.testProject1
      val destWorkspaceName = WorkspaceName(destBillingProject.projectName.value, "cool_workspace")
      val workspaceRequest = WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)

      val baseWorkspace = testData.workspace
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

      // Project ID gets allocated when creating the Workspace, so we don't care what it is here.  We do care that
      // we set the right Billing Account on it, which is the Billing Account specified by the Billing Project in the
      // clone Workspace Request
      verify(services.gcsDAO, times(1)).setBillingAccountName(
        any[GoogleProjectId],
        ArgumentMatchers.eq(destBillingProject.billingAccount.get),
        any[RawlsTracingContext]
      )
  }

  it should "fail to create a database object when GoogleServicesDAO throws an exception when updating billing account" in withTestDataServices {
    services =>
      val baseWorkspace = testData.workspace
      val destBillingProject = testData.testProject1
      val clonedWorkspaceName = WorkspaceName(destBillingProject.projectName.value, "sad_workspace")
      val cloneWorkspaceRequest = WorkspaceRequest(clonedWorkspaceName.namespace, clonedWorkspaceName.name, Map.empty)

      doReturn(Future.failed(new Exception("Fake error from Google")), null)
        .when(services.gcsDAO)
        .setBillingAccountName(
          ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
          ArgumentMatchers.eq(RawlsBillingAccountName("fakeBillingAcct")),
          any[RawlsTracingContext]
        )

      intercept[Exception] {
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       cloneWorkspaceRequest
                     ),
                     Duration.Inf
        )
      }

      val maybeWorkspace = runAndWait(workspaceQuery.findByName(clonedWorkspaceName))
      maybeWorkspace shouldBe None
  }

  it should "not try to modify the Service Perimeter if the Billing Project does not specify a Service Perimeter" in withTestDataServices {
    services =>
      val baseWorkspace = testData.workspace
      val newWorkspaceName = "space_for_workin"
      val billingProject = testData.testProject1

      // Pre-condition: make sure that the Billing Project we're adding the Workspace to DOES NOT specify a Service
      // Perimeter
      billingProject.servicePerimeter shouldBe empty

      val workspaceRequest = WorkspaceRequest(billingProject.projectName.value, newWorkspaceName, Map.empty)
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )

      // Verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was NOT called
      verify(services.googleAccessContextManagerDAO, Mockito.never())
        .overwriteProjectsInServicePerimeter(any[ServicePerimeterName], any[Set[String]])
  }

  it should "claim a Google Project from Resource Buffering Service" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    Await.result(
      services.workspaceService.cloneWorkspace(
        baseWorkspace.toWorkspaceName,
        workspaceRequest
      ),
      Duration.Inf
    )

    verify(services.resourceBufferService).getGoogleProjectFromBuffer(any[ProjectPoolType], any[String])
  }

  it should "clone a workspace bucket with enhanced logging, resulting in the child bucket having enhanced logging" in withTestDataServices {
    services =>
      val baseWorkspaceName = "secure_space_for_workin"
      val baseWorkspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        baseWorkspaceName,
        Map.empty,
        authorizationDomain = None,
        enhancedBucketLogging = Some(true)
      )
      val baseWorkspace = Await.result(services.workspaceService.createWorkspace(baseWorkspaceRequest), Duration.Inf)

      val newWorkspaceName = "cloned_space"
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

      val workspace =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "clone a workspace bucket with an Auth Domain, resulting in the child bucket having enhanced logging" in withTestDataServices {
    services =>
      val baseWorkspaceName = "secure_space_for_workin"
      val baseWorkspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        baseWorkspaceName,
        Map.empty,
        authorizationDomain = Option(Set(testData.dbGapAuthorizedUsersGroup))
      )
      val baseWorkspace = Await.result(services.workspaceService.createWorkspace(baseWorkspaceRequest), Duration.Inf)

      val newWorkspaceName = "cloned_space"
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

      val workspace =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "clone a workspace with an enhanced bucket monitoring, resulting in the child workspace having enhanced logging even if the destination bucket location is defined" in withTestDataServices {
    services =>
      val baseWorkspaceName = "secure_space_for_workin"
      val baseWorkspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        baseWorkspaceName,
        Map.empty,
        enhancedBucketLogging = Some(true)
      )
      val baseWorkspace = Await.result(services.workspaceService.createWorkspace(baseWorkspaceRequest), Duration.Inf)

      val newWorkspaceName = "cloned_space"
      val workspaceRequest =
        WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty, bucketLocation = Some("US"))

      val workspace =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "create a bucket with enhanced logging when told to, even if the parent workspace doesn't have it" in withTestDataServices {
    services =>
      val baseWorkspaceName = "secure_space_for_workin"
      val baseWorkspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        baseWorkspaceName,
        Map.empty,
        authorizationDomain = None
      )
      val baseWorkspace = Await.result(services.workspaceService.createWorkspace(baseWorkspaceRequest), Duration.Inf)

      val newWorkspaceName = "cloned_space"
      val workspaceRequest =
        WorkspaceRequest(testData.testProject1Name.value,
                         newWorkspaceName,
                         Map.empty,
                         enhancedBucketLogging = Some(true)
        )

      val workspace =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  it should "clone the WSM stub workspace if it exists" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    Await.result(
      services.workspaceService.cloneWorkspace(
        baseWorkspace.toWorkspaceName,
        workspaceRequest
      ),
      Duration.Inf
    )

    verify(services.workspaceService.workspaceManagerDAO).cloneWorkspace(
      ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
      any[UUID],
      any[String],
      ArgumentMatchers.eq(None),
      any[String],
      any[RawlsRequestContext],
      any[Option[WsmPolicyInputs]]
    )
  }

  it should "not fail if the source workspace doesn't have a WSM stub workspace" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    when(
      services.workspaceService.workspaceManagerDAO.cloneWorkspace(
        ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
        any[UUID],
        any[String],
        ArgumentMatchers.eq(None),
        any[String],
        any[RawlsRequestContext],
        any[Option[WsmPolicyInputs]]
      )
    ).thenThrow(new ApiException(StatusCodes.NotFound.intValue, "Rawls stage workspace not found"))

    Await.result(
      services.workspaceService.cloneWorkspace(
        baseWorkspace.toWorkspaceName,
        workspaceRequest
      ),
      Duration.Inf
    )

    verify(services.workspaceService.workspaceManagerDAO).cloneWorkspace(
      ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
      any[UUID],
      any[String],
      ArgumentMatchers.eq(None),
      any[String],
      any[RawlsRequestContext],
      any[Option[WsmPolicyInputs]]
    )
  }

  it should "fail if cloning the WSM stub workspace fails" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    when(
      services.workspaceService.workspaceManagerDAO.cloneWorkspace(
        ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
        any[UUID],
        any[String],
        ArgumentMatchers.eq(None),
        any[String],
        any[RawlsRequestContext],
        any[Option[WsmPolicyInputs]]
      )
    ).thenThrow(new ApiException(StatusCodes.InternalServerError.intValue, "kablooey"))

    val thrown = intercept[ApiException] {
      Await.result(services.workspaceService.cloneWorkspace(
                     baseWorkspace.toWorkspaceName,
                     workspaceRequest
                   ),
                   Duration.Inf
      )
    }

    verify(services.workspaceService.workspaceManagerDAO).cloneWorkspace(
      ArgumentMatchers.eq(baseWorkspace.workspaceIdAsUUID),
      any[UUID],
      any[String],
      ArgumentMatchers.eq(None),
      any[String],
      any[RawlsRequestContext],
      any[Option[WsmPolicyInputs]]
    )
    thrown.getCode shouldBe StatusCodes.InternalServerError.intValue
  }

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "cloning a Workspace into a Service Perimeter" should "attempt to overwrite the correct Service Perimeter" in withTestDataServices {
    services =>
      // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
      val servicePerimeterName: ServicePerimeterName =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters.keys.head
      val billingProject1 = testData.testProject1
      val billingProject2 = testData.testProject2
      val billingProjects = Seq(billingProject1, billingProject2)
      val workspacesPerProject = 2

      // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
      // the Billing Projects and therefore in the Perimeter
      billingProjects.flatMap { bp =>
        runAndWait {
          for {
            _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateServicePerimeter(bp.projectName,
                                                                                            servicePerimeterName.some
            )
            updatedBillingProject <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName)
          } yield updatedBillingProject.value.servicePerimeter.value shouldBe servicePerimeterName
        }

        (1 to workspacesPerProject).map { n =>
          val workspace = testData.workspace.copy(
            namespace = bp.projectName.value,
            name = s"${bp.projectName.value}Workspace$n",
            workspaceId = UUID.randomUUID().toString,
            googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
          )
          runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
        }
      }

      // Test setup is done, now we're getting to the test
      // Make a call to Create a new Workspace in the same Billing Project
      val baseWorkspace = testData.workspace
      val workspaceName = WorkspaceName(testData.testProject1Name.value, "cool_workspace")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
      val workspace =
        Await.result(services.workspaceService.cloneWorkspace(
                       baseWorkspace.toWorkspaceName,
                       workspaceRequest
                     ),
                     Duration.Inf
        )

      val servicePerimeterNameCaptor = captor[ServicePerimeterName]
      // verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was called exactly once and capture
      // the arguments passed to it so that we can verify that they were correct
      verify(services.servicePerimeterService).overwriteGoogleProjectsInPerimeter(servicePerimeterNameCaptor.capture,
                                                                                  any[DataAccess]
      )
      servicePerimeterNameCaptor.getValue shouldBe servicePerimeterName

      // verify that we set the folder for the perimeter
      verify(services.gcsDAO).addProjectToFolder(ArgumentMatchers.eq(workspace.googleProjectId), any[String])
  }

  behavior of "sendChangeNotifications"

  it should "send a workspace changed notification to all users" in withTestDataServices { services =>
    val service = services.workspaceService
    when(
      service.samDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                              ArgumentMatchers.eq(testData.workspace.workspaceId),
                                              any()
      )
    ).thenReturn(
      Future(
        Set(
          UserIdInfo(
            "User1Id",
            "user1@foo.com",
            Some("googleUser1Id")
          ),
          UserIdInfo(
            "User2Id",
            "user2@foo.com",
            Some("googleUser2Id")
          )
        )
      )
    )

    val notificationCaptor = captor[Set[Notifications.WorkspaceChangedNotification]]

    val numSent = Await.result(service.sendChangeNotifications(testData.workspace.toWorkspaceName), Duration.Inf)

    verify(services.notificationDAO, times(1)).fireAndForgetNotifications(notificationCaptor.capture)(
      ArgumentMatchers.any()
    )
    notificationCaptor.getValue.size shouldBe 2
    val firstNotification = notificationCaptor.getValue.head
    firstNotification.recipientUserId.value shouldEqual "googleUser1Id"
    firstNotification.workspaceName shouldEqual Notifications.WorkspaceName(testData.workspace.namespace,
                                                                            testData.workspace.name
    )
    val secondNotification = notificationCaptor.getValue.tail.head
    secondNotification.recipientUserId.value shouldEqual "googleUser2Id"
    secondNotification.workspaceName shouldEqual Notifications.WorkspaceName(testData.workspace.namespace,
                                                                             testData.workspace.name
    )

    numSent shouldBe "2"
  }

  behavior of "getAccessInstructions"

  it should "return instructions from Sam" in withTestDataServices { services =>
    val service = services.workspaceService

    when(
      service.samDAO.getResourceAuthDomain(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                           ArgumentMatchers.eq(testData.workspace.workspaceId),
                                           any()
      )
    ).thenReturn(Future(Seq("domain1", "domain2")))

    when(service.samDAO.getAccessInstructions(ArgumentMatchers.eq(WorkbenchGroupName("domain1")), any()))
      .thenReturn(Future(Some("instruction1")))

    when(service.samDAO.getAccessInstructions(ArgumentMatchers.eq(WorkbenchGroupName("domain2")), any()))
      .thenReturn(Future(Some("instruction2")))

    val instructions = Await.result(service.getAccessInstructions(testData.workspace.toWorkspaceName), Duration.Inf)

    instructions.length shouldBe 2
    instructions.head.groupName shouldBe "domain1"
    instructions.head.instructions shouldBe "instruction1"
    instructions.tail.head.groupName shouldBe "domain2"
    instructions.tail.head.instructions shouldBe "instruction2"
  }

  behavior of "getWorkspace"

  it should "get the details for a GCP workspace" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    val readWorkspace = Await.result(services.workspaceService.getWorkspace(
                                       WorkspaceName(workspace.namespace, workspace.name),
                                       WorkspaceFieldSpecs()
                                     ),
                                     Duration.Inf
    )

    val response = readWorkspace.convertTo[WorkspaceResponse]

    response.workspace.name shouldBe workspaceName
    response.workspace.namespace shouldBe testData.testProject1Name.value
    response.bucketOptions shouldBe Some(WorkspaceBucketOptions(false, services.gcsDAO.bucketLocation))
    response.azureContext shouldEqual None
    response.workspace.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    response.workspace.state shouldBe WorkspaceState.Ready
  }

  private def toTpsPao(objectId: UUID, policies: List[WsmPolicyInput]) =
    new TpsPaoGetResult()
      .objectId(objectId)
      .effectiveAttributes(
        new TpsPolicyInputs().inputs(
          policies
            .map(p =>
              new TpsPolicyInput()
                .name(p.getName)
                .namespace(p.getNamespace)
                .additionalData(
                  p.getAdditionalData.asScala
                    .map(pair => new TpsPolicyPair().key(pair.getKey).value(pair.getValue))
                    .asJava
                )
            )
            .asJava
        )
      )

  private def createGcpWorkspacePolicy(services: TestApiService,
                                       workspaceName: String,
                                       policies: List[WsmPolicyInput] = List(),
                                       workspaceService: WorkspaceService
  ): Workspace = {
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    val createdWorkspace = Await
      .result(
        workspaceService.createWorkspace(workspaceRequest),
        Duration.Inf
      )
    when(
      services.policyService.getPao(ArgumentMatchers.eq(createdWorkspace.workspaceIdAsUUID), any[RawlsRequestContext])
    ).thenReturn(Future.successful(Option(toTpsPao(createdWorkspace.workspaceIdAsUUID, policies))))
    when(
      services.policyService.listPaos(any, any[RawlsRequestContext])
    ).thenReturn(Future.successful(Seq(toTpsPao(createdWorkspace.workspaceIdAsUUID, policies))))

    createdWorkspace
  }

  it should "return the policies of a GCP workspace" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val wsmPolicyInput = new WsmPolicyInput()
      .name("test_name")
      .namespace("test_namespace")
      .additionalData(
        List(
          new WsmPolicyPair().value("pair1Val").key("pair1Key"),
          new WsmPolicyPair().value("pair2Val").key("pair2Key")
        ).asJava
      )
    val workspace = createGcpWorkspacePolicy(services, workspaceName, List(wsmPolicyInput), services.workspaceService)
    val readWorkspace = Await.result(services.workspaceService.getWorkspace(
                                       WorkspaceName(workspace.namespace, workspace.name),
                                       WorkspaceFieldSpecs()
                                     ),
                                     Duration.Inf
    )

    val response = readWorkspace.convertTo[WorkspaceResponse]

    response.workspace.name shouldBe workspaceName
    response.azureContext shouldEqual None
    response.workspace.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    response.policies should not be empty
    val policies: List[WorkspacePolicy] = response.policies.get
    policies should not be empty
    val policy: WorkspacePolicy = policies.head
    policy.name shouldBe wsmPolicyInput.getName
    policy.namespace shouldBe wsmPolicyInput.getNamespace
    val additionalData = policy.additionalData
    additionalData.length shouldEqual 2
    additionalData.head.getOrElse("pair1Key", "fail") shouldEqual "pair1Val"
    additionalData.tail.head.getOrElse("pair2Key", "fail") shouldEqual "pair2Val"
  }

  behavior of "listWorkspaces"

  it should "list the correct cloud platform and state for Google workspaces" in withTestDataServices { services =>
    val service = services.workspaceService
    val workspaceId1 = UUID.randomUUID().toString
    val workspaceId2 = UUID.randomUUID().toString

    // set up test data
    val googleWorkspace = Workspace("test_namespace2",
                                    workspaceId2,
                                    workspaceId2,
                                    "aBucket",
                                    Some("workflow-collection"),
                                    new DateTime(),
                                    new DateTime(),
                                    "testUser2",
                                    Map.empty
    )
    val googleWorkspaceDetails =
      WorkspaceDetails.fromWorkspaceAndOptions(googleWorkspace, Some(Set()), true, Some(WorkspaceCloudPlatform.Gcp))
    val expected = List(
      (googleWorkspaceDetails.workspaceId, googleWorkspaceDetails.cloudPlatform, googleWorkspaceDetails.state)
    )

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleWorkspace)
      } yield ()
    }

    // mock external calls
    when(service.samDAO.listUserResources(SamResourceTypeNames.workspace, services.ctx1)).thenReturn(
      Future(
        Seq(
          SamUserResource(
            workspaceId1,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          ),
          SamUserResource(
            workspaceId2,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          )
        )
      )
    )
    when(services.policyService.listPaos(any, any)).thenReturn(Future.successful(Seq.empty))

    // actually call listWorkspaces to get result it returns given the mocked calls you set up
    val result =
      Await
        .result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
        .convertTo[Seq[WorkspaceListResponse]]

    // verify that the result is what you expect it to be
    result.map(ws =>
      (ws.workspace.workspaceId, ws.workspace.cloudPlatform, ws.workspace.state)
    ) should contain theSameElementsAs expected
  }

  it should "return only the leftmost N characters of string attributes" in withTestDataServices { services =>
    val service = services.workspaceService
    val workspaceId1 = UUID.randomUUID().toString
    val workspaceId2 = UUID.randomUUID().toString

    val descriptionKey = AttributeName.withDefaultNS("description")

    val shortDescription = AttributeString("the quick brown fox jumped over the lazy dog")
    val longDescription = AttributeString("abcd" * 10000) // should be 40000 chars

    // set up test data
    val descriptive1 = Workspace(
      "test_namespace2",
      workspaceId1,
      workspaceId1,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map(descriptionKey -> shortDescription)
    )
    val descriptive2 = Workspace(
      "test_namespace2",
      workspaceId2,
      workspaceId2,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map(descriptionKey -> longDescription)
    )

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(descriptive1)
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(descriptive2)
      } yield ()
    }

    when(service.samDAO.listUserResources(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any())).thenReturn(
      Future(
        Seq(
          SamUserResource(
            workspaceId1,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          ),
          SamUserResource(
            workspaceId2,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          )
        )
      )
    )
    when(services.policyService.listPaos(any, any)).thenReturn(Future.successful(Seq.empty))

    List(0, 1, 10, 200, 4096) foreach { stringAttributeMaxLength =>
      info(s"for stringAttributeMaxLength = $stringAttributeMaxLength")
      val result =
        Await
          .result(service.listWorkspaces(WorkspaceFieldSpecs(), stringAttributeMaxLength), Duration.Inf)
          .convertTo[Seq[WorkspaceListResponse]]

      result.map { ws =>
        val actualAttributes = ws.workspace.attributes.getOrElse(Map())
        actualAttributes.keySet should contain(descriptionKey)
        val actual = actualAttributes.getOrElse(descriptionKey, AttributeNull)
        actual match {
          case AttributeString(s) =>
            s.length should be <= stringAttributeMaxLength
          case x => fail(s"description attribute was returned as a ${x.getClass.getSimpleName}")
        }
      }
    }
  }

  it should "return entire string attributes when stringAttributeMaxLength = -1" in withTestDataServices { services =>
    val service = services.workspaceService
    val workspaceId1 = UUID.randomUUID().toString
    val workspaceId2 = UUID.randomUUID().toString

    val descriptionKey = AttributeName.withDefaultNS("description")

    val shortDescription = AttributeString("the quick brown fox jumped over the lazy dog")
    val longDescription = AttributeString("abcd" * 10000) // should be 40000 chars

    // set up test data
    val descriptive1 = Workspace(
      "test_namespace2",
      workspaceId1,
      workspaceId1,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map(descriptionKey -> shortDescription)
    )
    val descriptive2 = Workspace(
      "test_namespace2",
      workspaceId2,
      workspaceId2,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map(descriptionKey -> longDescription)
    )

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(descriptive1)
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(descriptive2)
      } yield ()
    }

    when(service.samDAO.listUserResources(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any())).thenReturn(
      Future(
        Seq(
          SamUserResource(
            workspaceId1,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          ),
          SamUserResource(
            workspaceId2,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          )
        )
      )
    )
    when(services.policyService.listPaos(any, any)).thenReturn(Future.successful(Seq.empty))

    val result =
      Await
        .result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
        .convertTo[Seq[WorkspaceListResponse]]

    result.map { ws =>
      val actualAttributes = ws.workspace.attributes.getOrElse(Map())
      actualAttributes.keySet should contain(descriptionKey)
      val actual = actualAttributes.getOrElse(descriptionKey, AttributeNull)
      if (ws.workspace.workspaceId == workspaceId1) {
        actual shouldBe shortDescription
      } else {
        actual shouldBe longDescription
      }
    }
  }

  it should "return numbers unchanged when specifying stringAttributeMaxLength" in withTestDataServices { services =>
    val service = services.workspaceService
    val workspaceId1 = UUID.randomUUID().toString

    val descriptionKey = AttributeName.withDefaultNS("description")
    val numberKey = AttributeName.withDefaultNS("iamanumber")

    val shortDescription = AttributeString("the quick brown fox jumped over the lazy dog")
    val numberAttr = AttributeNumber(123456789)

    // set up test data
    val descriptive1 = Workspace(
      "test_namespace2",
      workspaceId1,
      workspaceId1,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map(descriptionKey -> shortDescription, numberKey -> numberAttr)
    )

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(descriptive1)
      } yield ()
    }

    when(service.samDAO.listUserResources(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any())).thenReturn(
      Future(
        Seq(
          SamUserResource(
            workspaceId1,
            SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          )
        )
      )
    )
    when(services.policyService.listPaos(any, any)).thenReturn(Future.successful(Seq.empty))

    val stringAttributeMaxLength = 5

    val result =
      Await
        .result(service.listWorkspaces(WorkspaceFieldSpecs(), stringAttributeMaxLength), Duration.Inf)
        .convertTo[Seq[WorkspaceListResponse]]

    result.map { ws =>
      val actualAttributes = ws.workspace.attributes.getOrElse(Map())
      actualAttributes.keySet should contain(descriptionKey)
      actualAttributes.keySet should contain(numberKey)
      val actualDescription = actualAttributes.getOrElse(descriptionKey, AttributeNull)
      val actualNumber = actualAttributes.getOrElse(numberKey, AttributeNull)

      actualDescription match {
        case AttributeString(s) =>
          s.length shouldBe stringAttributeMaxLength
        case x => fail(s"description attribute was returned as a ${x.getClass.getSimpleName}")
      }

      actualNumber shouldBe numberAttr
    }
  }

  it should "return policy information for GCP workspaces with a stub workspace" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val wsmPolicyInput = new WsmPolicyInput()
      .name("gcp_test_name")
      .namespace("gcp_test_namespace")
      .additionalData(
        List(
          new WsmPolicyPair().value("pair1Val").key("pair1Key")
        ).asJava
      )
    createGcpWorkspacePolicy(services, workspaceName, List(wsmPolicyInput), services.workspaceService)

    val result = Await
      .result(services.workspaceService.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
      .convertTo[Seq[WorkspaceListResponse]]

    val matchingWorkspaces = result.filter { ws =>
      if (ws.workspace.name == workspaceName) {
        val policies: List[WorkspacePolicy] = ws.policies.get
        policies should not be empty
        val policy: WorkspacePolicy = policies.head
        policy.name shouldBe wsmPolicyInput.getName
        policy.namespace shouldBe wsmPolicyInput.getNamespace
        true
      } else {
        false
      }
    }
    matchingWorkspaces.size should be(1)
  }

  it should "return canCompute and canShare for Google workspaces" in withTestDataServices { services =>
    val service = services.workspaceService

    // set up test data
    val googleShareWriterNoComputeWorkspace = Workspace(
      "googleWriterNoComputeNamespace",
      "googleWriterNoComputeWorkspace",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map.empty
    )
    val googleWriterCanComputeWorkspace = Workspace(
      "googleWriterCanComputeNamespace",
      "googleWriterCanComputeWorkspace",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map.empty
    )
    val googleReaderWorkspace = Workspace(
      "googleReaderNamespace",
      "googleReaderWorkspace",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      new DateTime(),
      new DateTime(),
      "testUser2",
      Map.empty
    )
    val expected = List(
      (googleReaderWorkspace.name, Some(false), Some(false)), // does not have share-reader
      (googleShareWriterNoComputeWorkspace.name, Some(false), Some(true)), // share-writer added
      (googleWriterCanComputeWorkspace.name, Some(true), Some(false)) // does not have share-writer
    )

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleReaderWorkspace)
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleShareWriterNoComputeWorkspace)
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleWriterCanComputeWorkspace)
      } yield ()
    }

    // mock external calls
    when(service.samDAO.listUserResources(SamResourceTypeNames.workspace, services.ctx1)).thenReturn(
      Future(
        Seq(
          SamUserResource(
            googleReaderWorkspace.workspaceId,
            SamRolesAndActions(Set(SamWorkspaceRoles.reader), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          ),
          SamUserResource(
            googleShareWriterNoComputeWorkspace.workspaceId,
            SamRolesAndActions(Set(SamWorkspaceRoles.writer, SamWorkspaceRoles.shareWriter), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          ),
          SamUserResource(
            googleWriterCanComputeWorkspace.workspaceId,
            SamRolesAndActions(Set(SamWorkspaceRoles.writer, SamWorkspaceRoles.canCompute), Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            SamRolesAndActions(Set.empty, Set.empty),
            Set.empty,
            Set.empty
          )
        )
      )
    )
    when(services.policyService.listPaos(any, any)).thenReturn(Future.successful(Seq.empty))

    // actually call listWorkspaces to get result it returns given the mocked calls you set up
    val result =
      Await
        .result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
        .convertTo[Seq[WorkspaceListResponse]]

    // verify that the result is what you expect it to be
    result.map(ws => (ws.workspace.name, ws.canCompute, ws.canShare)) should contain theSameElementsAs expected
  }

  "checkWorkspaceCloudPermissions" should "use workspace pet for > reader" in withTestDataServices { services =>
    Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                 Duration.Inf
    )
    verify(services.samDAO).getPetServiceAccountKeyForUser(testData.workspace.googleProjectId, userInfo.userEmail)
  }

  it should "find missing bucket permissions" in withTestDataServices { services =>
    val storageRole = "storage.foo"
    when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraBucketWriterRole))
      .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(storageRole).asJava))))
    when(
      services.gcsDAO.testSAGoogleBucketIam(any[GcsBucketName], any[String], any[Set[IamPermission]])(
        any[ExecutionContext]
      )
    ).thenReturn(Future.successful(Set.empty))
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }

    err.errorReport.message should include(storageRole)
  }

  it should "find missing project permissions" in withTestDataServices { services =>
    val projectRole = "some.role"
    when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraWorkspaceCanComputeRole))
      .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(projectRole).asJava))))
    when(
      services.gcsDAO.testSAGoogleProjectIam(any[GoogleProject], any[String], any[Set[IamPermission]])(
        any[ExecutionContext]
      )
    ).thenReturn(Future.successful(Set.empty))
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }

    err.errorReport.message should include(projectRole)
  }

  it should "use default pet for reader" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                 Duration.Inf
    )
    verify(services.samDAO).getDefaultPetServiceAccountKeyForUser(any[RawlsRequestContext])
  }

  it should "require read access" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    Await.result(
      services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        testData.workspace.workspaceId,
        SamWorkspacePolicyNames.reader,
        SamPolicy(Set.empty, Set(SamWorkspaceActions.read), Set(SamWorkspaceRoles.reader)),
        testContext
      ),
      Duration.Inf
    )
    intercept[NoSuchWorkspaceException] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }
  }

  it should "rethrow IAMPermission errors with original status code" in withTestDataServices { services =>
    val storageRole = "storage.foo"
    val mockErrorMessage = "Mock bad billing response"
    when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraBucketWriterRole))
      .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(storageRole).asJava))))
    when(
      services.gcsDAO.testSAGoogleBucketIam(any[GcsBucketName], any[String], any[Set[IamPermission]])(
        any[ExecutionContext]
      )
    ).thenReturn(Future.failed(new StorageException(403, mockErrorMessage)))
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }

    err.errorReport.message should include(mockErrorMessage)
    err.errorReport.statusCode.get shouldBe StatusCodes.Forbidden
  }

  it should "rethrow IAMPermission errors that have unsupported status codes" in withTestDataServices { services =>
    val storageRole = "storage.foo"
    val mockErrorMessage = "Mock bad billing response"
    val mockJsonError = new GoogleJsonError().set("message", mockErrorMessage)

    when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraBucketWriterRole))
      .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(storageRole).asJava))))
    when(
      services.gcsDAO.testSAGoogleBucketIam(any[GcsBucketName], any[String], any[Set[IamPermission]])(
        any[ExecutionContext]
      )
    ).thenReturn(
      Future.failed(
        new GoogleJsonResponseException(new HttpResponseException.Builder(498, "", new HttpHeaders()), mockJsonError)
      )
    )
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }

    err.errorReport.message should include(mockErrorMessage)
    err.errorReport.statusCode.get.intValue() shouldBe 498
  }

  it should "rethrow IOEExceptions from testSAGoogleProjectIam with status 400" in withTestDataServices { services =>
    val projectRole = "some.role"
    val mockErrorMessage = "Mock project IAM error"
    when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraWorkspaceCanComputeRole))
      .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(projectRole).asJava))))
    when(
      services.gcsDAO.testSAGoogleProjectIam(any[GoogleProject], any[String], any[Set[IamPermission]])(
        any[ExecutionContext]
      )
    ).thenReturn(Future.failed(new IOException(mockErrorMessage)))
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                   Duration.Inf
      )
    }

    err.errorReport.message should include(mockErrorMessage)
    // Always throws with 400 because there is not a good way to get at the original status code.
    // The important thing here is that we don't want the exception to percolate up
    // and get thrown as a 500, which will cause Sentry error notifications.
    err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "rethrow GoogleJsonResponseExceptions from testSAGoogleProjectIam handling status code outside of normal range" in withTestDataServices {
    services =>
      val projectRole = "some.role"
      val mockErrorMessage = "Mock bad billing response"
      val mockJsonError = new GoogleJsonError().set("message", mockErrorMessage)

      when(services.googleIamDAO.getOrganizationCustomRole(services.workspaceService.terraWorkspaceCanComputeRole))
        .thenReturn(Future.successful(Option(new Role().setIncludedPermissions(List(projectRole).asJava))))
      when(
        services.gcsDAO.testSAGoogleProjectIam(any[GoogleProject], any[String], any[Set[IamPermission]])(
          any[ExecutionContext]
        )
      ).thenReturn(
        Future.failed(
          new GoogleJsonResponseException(new HttpResponseException.Builder(998, "", new HttpHeaders()), mockJsonError)
        )
      )
      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.checkWorkspaceCloudPermissions(testData.workspace.toWorkspaceName),
                     Duration.Inf
        )
      }

      err.errorReport.message should include(mockErrorMessage)
      err.errorReport.statusCode.get.intValue() shouldBe 998
  }

  "addAuthDomainGroups" should "call addResourceAuthDomain when adding new AD groups" in withTestDataServices {
    services =>
      val workspace = runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(testData.workspace))

      when(
        services.samDAO.getResourceAuthDomain(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(workspace.workspaceId),
          any
        )
      ).thenReturn(Future.successful(Seq("group1", "group2")))

      val testGroups = Set("group3")
      when(
        services.samDAO.addResourceAuthDomain(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(workspace.workspaceId),
          ArgumentMatchers.eq(testGroups),
          any
        )
      ).thenReturn(Future.successful(()))

      Await.result(services.workspaceService.addAuthDomainGroups(workspace.toWorkspaceName,
                                                                 testGroups + "group2",
                                                                 toRawlsRequestContext(testData.userOwner)
                   ),
                   Duration.Inf
      )

      verify(services.samDAO).addResourceAuthDomain(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        ArgumentMatchers.eq(testGroups),
        any
      )
      verify(services.gcsDAO, never).changeProjectOwnerBucketIamBinding(any, any, any)
  }

  it should "403 adding new AD groups without permissions" in withTestDataServices { services =>
    val workspace = runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(testData.workspace))

    when(
      services.samDAO.getResourceAuthDomain(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        any
      )
    ).thenReturn(Future.successful(Seq("group1", "group2")))
    when(
      services.samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        ArgumentMatchers.eq(SamWorkspaceActions.updateAuthDomain),
        any
      )
    ).thenReturn(Future.successful(false))

    val testGroups = Set("group3")
    intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.addAuthDomainGroups(workspace.toWorkspaceName,
                                                                 testGroups,
                                                                 toRawlsRequestContext(testData.userOwner)
                   ),
                   Duration.Inf
      )
    }.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

    verify(services.samDAO, never).addResourceAuthDomain(any, any, any, any)
    verify(services.gcsDAO, never).changeProjectOwnerBucketIamBinding(any, any, any)
  }

  it should "call changeProjectOwnerBucketIamBinding when adding first AD group" in withTestDataServices { services =>
    val workspace = runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(testData.workspace))

    when(
      services.samDAO.getResourceAuthDomain(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        any
      )
    ).thenReturn(Future.successful(Seq.empty))

    val testGroups = Set("group3")
    when(
      services.samDAO.addResourceAuthDomain(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        ArgumentMatchers.eq(testGroups),
        any
      )
    ).thenReturn(Future.successful(()))

    val workspaceProjectOwnerEmail = "workspace email"
    when(
      services.samDAO.listPoliciesForResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        any
      )
    ).thenReturn(
      Future.successful(
        Set(
          SamPolicyWithNameAndEmail(
            SamWorkspacePolicyNames.projectOwner,
            SamPolicy(Set.empty, Set.empty, Set.empty),
            WorkbenchEmail(workspaceProjectOwnerEmail)
          )
        )
      )
    )
    val billingOwnerEmail = "billing email"
    when(
      services.samDAO.getPolicySyncStatus(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(workspace.namespace),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner),
        any
      )
    ).thenReturn(
      Future.successful(
        SamPolicySyncStatus(
          "",
          WorkbenchEmail(billingOwnerEmail)
        )
      )
    )

    when(
      services.gcsDAO.changeProjectOwnerBucketIamBinding(
        GcsBucketName(workspace.bucketName),
        Identity.group(billingOwnerEmail),
        Identity.group(workspaceProjectOwnerEmail)
      )
    ).thenReturn(Future.successful(()))

    Await.result(services.workspaceService.addAuthDomainGroups(workspace.toWorkspaceName,
                                                               testGroups,
                                                               toRawlsRequestContext(testData.userOwner)
                 ),
                 Duration.Inf
    )

    verify(services.samDAO).addResourceAuthDomain(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspace.workspaceId),
      ArgumentMatchers.eq(testGroups),
      any
    )
    verify(services.gcsDAO).changeProjectOwnerBucketIamBinding(
      GcsBucketName(workspace.bucketName),
      Identity.group(billingOwnerEmail),
      Identity.group(workspaceProjectOwnerEmail)
    )
  }

  it should "no op if no new AD groups" in withTestDataServices { services =>
    val workspace = runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(testData.workspace))

    val testGroups = Set("group3")
    when(
      services.samDAO.getResourceAuthDomain(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        any
      )
    ).thenReturn(Future.successful(testGroups.toSeq))

    Await.result(services.workspaceService.addAuthDomainGroups(workspace.toWorkspaceName,
                                                               testGroups,
                                                               toRawlsRequestContext(testData.userOwner)
                 ),
                 Duration.Inf
    )

    verify(services.samDAO, never).addResourceAuthDomain(any, any, any, any)
    verify(services.gcsDAO, never).changeProjectOwnerBucketIamBinding(any, any, any)
  }

  "validateBillingProjectUpdate" should "not update workspace billing when source and destination billing are the same" in withTestDataServices {
    services =>
      val namespace = "testNamespace"
      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.workspaceService.validateBillingProjectUpdate(WorkspaceName(namespace, "test-workspace"), namespace),
          Duration.Inf
        )
      }
      err.errorReport.message should include(s"Workspace billing is already set to $namespace")
      err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "not update workspace billing when workspace does not exist" in withTestDataServices { services =>
    val workspaceName = WorkspaceName(testData.billingProject.projectName.value, "fakeWorkspace")
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        services.workspaceService.validateBillingProjectUpdate(workspaceName, testData.testProject1.projectName.value),
        Duration.Inf
      )
    }
    err.errorReport.message should include(s"Workspace ${workspaceName.name} does not exist")
    err.errorReport.statusCode.get shouldBe StatusCodes.NotFound
  }

  it should "not update workspace billing when either billing account does not exist" in withTestDataServices {
    services =>
      val workspaceName = testData.workspace.toWorkspaceName
      val destBilling = RawlsBillingProjectName("fakeBillingProject")
      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.validateBillingProjectUpdate(workspaceName, destBilling.value),
                     Duration.Inf
        )
      }
      err.errorReport.message should include(s"Billing Project $destBilling does not exist")
      err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "not update workspace billing when either billing account is Azure" in withTestDataServices { services =>
    val workspaceName = testData.workspace.toWorkspaceName
    val destBilling = testData.azureBillingProject.projectName
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.validateBillingProjectUpdate(workspaceName, destBilling.value),
                   Duration.Inf
      )
    }
    err.errorReport.message should include(s"Billing Project $destBilling does not exist")
    err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "not update workspace billing if user is not a billing project owner" in withTestDataServices { services =>
    val workspace = testData.workspace
    val sourceProject = testData.billingProject
    val targetProject = testData.testProject1

    when(
      services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject,
                                               sourceProject.projectName.value,
                                               services.workspaceService.ctx
      )
    )
      .thenReturn(Future.successful(Set.empty))

    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.validateBillingProjectUpdate(workspace.toWorkspaceName,
                                                                          targetProject.projectName.value
                   ),
                   Duration.Inf
      )
    }
    err.errorReport.message should include(
      s"Missing ${SamBillingProjectRoles.owner} role on billing project '${sourceProject.projectName}'."
    )
    err.errorReport.statusCode.get shouldBe StatusCodes.Forbidden
  }

  it should "not update workspace billing if source and destination billing have different service perimeters" in withTestDataServices {
    services =>
      val workspace = testData.workspace
      val targetBilling = testData.testProject1

      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.updateServicePerimeter(
            targetBilling.projectName,
            servicePerimeter = Option(ServicePerimeterName("test-service-perimeter"))
          )
          _ <- slickDataSource.dataAccess.rawlsBillingProjectQuery.load(targetBilling.projectName)
        } yield ()
      }

      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.workspaceService.validateBillingProjectUpdate(workspace.toWorkspaceName,
                                                                 targetBilling.projectName.value
          ),
          Duration.Inf
        )
      }
      err.errorReport.message should include(
        s"Source and destination billing must have the same service perimeter, if any"
      )
      err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "not update workspace billing if the destination billing namespace already has a workspace with the same workspace name" in withTestDataServices {
    services =>
      val workspaceName = testData.workspace.toWorkspaceName
      val targetProject = testData.testProject2 // Test project that contains a workspace with workspace.name

      val workspaceWithSameName = testData.workspace.copy(
        namespace = targetProject.projectName.value,
        workspaceId = UUID.randomUUID().toString,
        googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
      )
      runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspaceWithSameName))

      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.workspaceService.validateBillingProjectUpdate(workspaceName, targetProject.projectName.value),
          Duration.Inf
        )
      }
      err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
      err.errorReport.message should include(
        s"Workspace ${workspaceName.name} already exists under billing project ${targetProject.projectName.value}"
      )
  }

  it should "not update workspace billing if destination billing is not enabled" in withTestDataServices { services =>
    when(services.gcsDAO.isBillingAccountEnabled(testData.testProject1.billingAccount.get))
      .thenReturn(Future.successful(false))

    val workspaceName = testData.workspace.toWorkspaceName
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        services.workspaceService.validateBillingProjectUpdate(workspaceName, testData.testProject1.projectName.value),
        Duration.Inf
      )
    }
    err.errorReport.message should include(
      s"Billing account ${testData.testProject1.billingAccount.get} is not enabled"
    )
    err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "not update workspace billing if workspace project does not have Google storage APIs enabled" in withTestDataServices {
    services =>
      val storageAPIs = List(
        "storage-api.googleapis.com",
        "storage-component.googleapis.com"
      )
      when(services.gcsDAO.isBillingAccountEnabled(testData.workspace.currentBillingAccountOnGoogleProject.get))
        .thenReturn(Future.successful(true))
      when(services.gcsDAO.areServicesEnabled(GoogleProject(testData.workspace.googleProjectId.value), storageAPIs))
        .thenReturn(false)

      val err = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          services.workspaceService.validateBillingProjectUpdate(testData.workspace.toWorkspaceName,
                                                                 testData.testProject1.projectName.value
          ),
          Duration.Inf
        )
      }
      err.errorReport.message should include(
        s"Required GCS APIs ${storageAPIs.toString()} are not enabled on project ${testData.workspace.googleProjectId.value}."
      )
      err.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
  }

  it should "succeed when all validation checks are met" in withTestDataServices { services =>
    val workspace = testData.workspace
    val targetBilling = testData.testProject1
    services.workspaceService.validateBillingProjectUpdate(workspace.toWorkspaceName, "target-billing").map {
      case (ws, project) =>
        ws shouldBe workspace
        project.projectName.value shouldBe targetBilling.projectName.value
    }
  }

  "updateWorkspaceBilling" should "successfully update workspace billing" in withTestDataServicesCustomFastPass {
    services =>
      val workspaceName = testData.workspace.toWorkspaceName
      val workspace = Await.result(services.workspaceRepository.getWorkspace(workspaceName), Duration.Inf).get
      val targetBilling = testData.testProject1

      val updatedWorkspace =
        Await
          .result(services.workspaceService.updateWorkspaceBilling(workspace, targetBilling), Duration.Inf)
          .get

      val oldBillingProjectOwnerPolicyEmail = Await.result(
        services.samDAO
          .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                               workspace.namespace,
                               SamBillingProjectPolicyNames.owner,
                               services.workspaceService.ctx
          )
          .map(_.email),
        Duration.Inf
      )

      val newBillingProjectOwnerPolicyEmail = Await.result(
        services.samDAO
          .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                               targetBilling.projectName.value,
                               SamBillingProjectPolicyNames.owner,
                               services.workspaceService.ctx
          )
          .map(_.email),
        Duration.Inf
      )

      verifyGCPBillingUpdate(workspace,
                             oldBillingProjectOwnerPolicyEmail,
                             newBillingProjectOwnerPolicyEmail,
                             targetBilling.billingAccount,
                             services
      )
      val mockRawlsSAContext = services.samDAO.rawlsSAContext
      verify(services.samDAO).addUserToPolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.projectOwner,
        newBillingProjectOwnerPolicyEmail.value,
        mockRawlsSAContext
      )
      verify(services.samDAO).removeUserFromPolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.projectOwner,
        oldBillingProjectOwnerPolicyEmail.value,
        mockRawlsSAContext
      )
      verify(services.mockFastPassService).removeFastPassGrantsForWorkspace(workspace)
      verify(services.mockFastPassService).syncFastPassesForUserInWorkspace(workspace)

      val workspaceFastPassGrants =
        runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

      val samUserStatus = Await
        .result(services.fastPassMockSamDAO.getUserIdInfoForEmail(WorkbenchEmail(services.user.userEmail.value)),
                Duration.Inf
        )
      val userSubjectId = WorkbenchUserId(samUserStatus.userSubjectId)

      val ownerRoles = Vector(
        services.terraWorkspaceCanComputeRole,
        services.terraWorkspaceNextflowRole,
        services.terraBucketWriterRole
      )
      workspaceFastPassGrants should not be empty
      workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)
      workspaceFastPassGrants.map(_.userSubjectId) should contain only userSubjectId

      // Verify Rawls updates
      updatedWorkspace.namespace shouldBe targetBilling.projectName.value
      updatedWorkspace.currentBillingAccountOnGoogleProject shouldBe targetBilling.billingAccount
  }

  def verifySamUpdate(oldBillingProjectOwnerPolicyEmail: WorkbenchEmail,
                      newBillingProjectOwnerPolicyEmail: WorkbenchEmail,
                      services: TestApiService
  ): Unit = {
    verify(services.samDAO, atLeastOnce()).addUserToPolicy(
      SamResourceTypeNames.workspace,
      workspace.workspaceId,
      SamWorkspacePolicyNames.projectOwner,
      newBillingProjectOwnerPolicyEmail.value,
      services.samDAO.rawlsSAContext
    )
    verify(services.samDAO, atLeastOnce()).removeUserFromPolicy(
      SamResourceTypeNames.workspace,
      workspace.workspaceId,
      SamWorkspacePolicyNames.projectOwner,
      oldBillingProjectOwnerPolicyEmail.value,
      services.samDAO.rawlsSAContext
    )
  }

  it should "revert workspace billing update if update fails in GCP" in withTestDataServices { services =>
    val workspaceName = testData.workspace.toWorkspaceName
    val workspace = Await.result(services.workspaceRepository.getWorkspace(workspaceName), Duration.Inf).get
    val targetBilling = testData.testProject1

    doReturn(Future.failed(new Exception("Fake error from Google")), null)
      .when(services.gcsDAO)
      .setBillingAccountName(
        ArgumentMatchers.eq(workspace.googleProjectId),
        ArgumentMatchers.eq(targetBilling.billingAccount.get),
        any[RawlsTracingContext]
      )

    val oldBillingProjectOwnerPolicyEmail = Await.result(
      services.samDAO
        .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                             workspace.namespace,
                             SamBillingProjectPolicyNames.owner,
                             services.workspaceService.ctx
        )
        .map(_.email),
      Duration.Inf
    )

    val newBillingProjectOwnerPolicyEmail = Await.result(
      services.samDAO
        .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                             targetBilling.projectName.value,
                             SamBillingProjectPolicyNames.owner,
                             services.workspaceService.ctx
        )
        .map(_.email),
      Duration.Inf
    )

    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.updateWorkspaceBilling(workspace, targetBilling), Duration.Inf)
    }

    verifyGCPBillingUpdate(workspace,
                           oldBillingProjectOwnerPolicyEmail,
                           newBillingProjectOwnerPolicyEmail,
                           targetBilling.billingAccount,
                           services
    )
    verifyGCPBillingUpdate(workspace,
                           newBillingProjectOwnerPolicyEmail,
                           oldBillingProjectOwnerPolicyEmail,
                           workspace.currentBillingAccountOnGoogleProject,
                           services
    )

    err.errorReport.message should include("Billing update failed in GCP")
    err.errorReport.statusCode.get shouldBe StatusCodes.InternalServerError
  }

  def verifyGCPBillingUpdate(workspace: Workspace,
                             oldBillingProjectOwnerPolicyEmail: WorkbenchEmail,
                             newBillingProjectOwnerPolicyEmail: WorkbenchEmail,
                             targetBillingAccount: Option[RawlsBillingAccountName],
                             services: TestApiService
  ): Unit = {
    val projectIAMRoles = Set(
      services.workspaceService.terraBillingProjectOwnerRole,
      services.workspaceService.terraWorkspaceCanComputeRole,
      services.workspaceService.terraWorkspaceNextflowRole
    )
    verify(services.gcsDAO, atLeastOnce()).changeProjectOwnerBucketIamBinding(any(), any(), any())
    verify(services.googleIamDAO, atLeastOnce()).addRoles(GoogleProject(workspace.googleProjectId.value),
                                                          newBillingProjectOwnerPolicyEmail,
                                                          IamMemberTypes.Group,
                                                          projectIAMRoles,
                                                          retryIfGroupDoesNotExist = true
    )
    verify(services.googleIamDAO, atLeastOnce()).removeRoles(GoogleProject(workspace.googleProjectId.value),
                                                             oldBillingProjectOwnerPolicyEmail,
                                                             IamMemberTypes.Group,
                                                             projectIAMRoles,
                                                             retryIfGroupDoesNotExist = true
    )
    verify(services.gcsDAO, atLeastOnce()).setBillingAccount(workspace.googleProjectId,
                                                             targetBillingAccount,
                                                             services.workspaceService.ctx.toTracingContext
    )
  }

  it should "revert workspace billing update if update fails in Sam" in withTestDataServices { services =>
    val workspaceName = testData.workspace.toWorkspaceName
    val workspace = Await.result(services.workspaceRepository.getWorkspace(workspaceName), Duration.Inf).get
    val targetBilling = testData.testProject1

    val oldBillingProjectOwnerPolicyEmail = Await.result(
      services.samDAO
        .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                             workspace.namespace,
                             SamBillingProjectPolicyNames.owner,
                             services.workspaceService.ctx
        )
        .map(_.email),
      Duration.Inf
    )

    val newBillingProjectOwnerPolicyEmail = Await.result(
      services.samDAO
        .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                             targetBilling.projectName.value,
                             SamBillingProjectPolicyNames.owner,
                             services.workspaceService.ctx
        )
        .map(_.email),
      Duration.Inf
    )

    doReturn(Future.failed(new Exception("Fake error from Sam")), Future.successful())
      .when(services.samDAO)
      .addUserToPolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspace.workspaceId),
        ArgumentMatchers.eq(SamWorkspacePolicyNames.projectOwner),
        ArgumentMatchers.eq(newBillingProjectOwnerPolicyEmail.value),
        any[RawlsRequestContext]
      )
    val err = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.updateWorkspaceBilling(workspace, targetBilling), Duration.Inf)
    }
    err.errorReport.message should include("Billing update failed in Sam")
    err.errorReport.statusCode.get shouldBe StatusCodes.InternalServerError

    verifySamUpdate(oldBillingProjectOwnerPolicyEmail, newBillingProjectOwnerPolicyEmail, services)
    verifySamUpdate(newBillingProjectOwnerPolicyEmail, oldBillingProjectOwnerPolicyEmail, services)

  }

}
