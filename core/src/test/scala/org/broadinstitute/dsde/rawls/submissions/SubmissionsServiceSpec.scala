package org.broadinstitute.dsde.rawls.submissions

import akka.actor.PoisonPill
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAOImpl
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.fastpass.FastPassServiceImpl
import org.broadinstitute.dsde.rawls.genomics.GenomicsServiceImpl
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationService
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferServiceImpl
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterServiceImpl
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.{
  MultiCloudWorkspaceAclManager,
  MultiCloudWorkspaceService,
  RawlsWorkspaceAclManager,
  WorkspaceRepository,
  WorkspaceService
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.DurationConverters.JavaDurationOps
import scala.language.postfixOps

class SubmissionsServiceSpec
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
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit
    override val executionContext: ExecutionContext
  ) extends WorkspaceApiService
      with MethodConfigApiService
      with SubmissionApiService
      with MockUserInfoDirectivesWithUser {
    val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))

    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(ctx1)
    lazy val methodConfigurationService: MethodConfigurationService = methodConfigurationServiceConstructor(ctx1)
    lazy val submissionsService: SubmissionsService = submissionsServiceConstructor(ctx1)
    lazy val userService: UserService = userServiceConstructor(ctx1)
    val slickDataSource: SlickDataSource = dataSource

    def actorRefFactory = system

    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val googleAccessContextManagerDAO = spy(new MockGoogleAccessContextManagerDAO())
    val gcsDAO = spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val googleIamDAO: MockGoogleIamDAO = spy(new MockGoogleIamDAO)
    val googleStorageDAO: MockGoogleStorageDAO = spy(new MockGoogleStorageDAO)
    val samDAO = spy(new MockSamDAO(dataSource))
    val gpsDAO = new org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
    val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val leonardoService = mock[LeonardoService](RETURNS_SMART_NULLS)
    when(
      leonardoService.cleanupResources(any[GoogleProjectId], any[UUID], any[RawlsRequestContext])(any[ExecutionContext])
    )
      .thenReturn(Future.successful())
    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO(mockServer.mockServerBaseUrl)

    val notificationTopic = "test-notification-topic"
    val notificationDAO = spy(new PubSubNotificationDAO(gpsDAO, notificationTopic))

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
          mockNotificationDAO,
          SubmissionMonitorConfig(1 second, 30 days, true, 20000, true),
          testConf.getDuration("entities.queryTimeout").toScala,
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
      workspaceManagerDAO,
      dataRepoDAO,
      samDAO,
      bigQueryServiceFactory,
      DataRepoEntityProviderConfig(100, 10, 0),
      testConf.getBoolean("entityStatisticsCache.enabled"),
      testConf.getDuration("entities.queryTimeout"),
      workbenchMetricBaseName
    )

    val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
    val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
    val resourceBufferService = spy(new ResourceBufferServiceImpl(resourceBufferDAO, resourceBufferConfig))
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
      fastPassServiceConstructor
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
        workspaceRepository
      ) _

    def cleanupSupervisor =
      submissionSupervisor ! PoisonPill
  }

  class TestApiServiceWithCustomSamDAO(dataSource: SlickDataSource, override val user: RawlsUser)(implicit
    override val executionContext: ExecutionContext
  ) extends TestApiService(dataSource, user) {
    override val samDAO: CustomizableMockSamDAO = spy(new CustomizableMockSamDAO(dataSource))

    // these need to be overridden to use the new samDAO
    override val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    override val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)
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

  def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)(executionContext)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  private def withServicesCustomSam[T](dataSource: SlickDataSource, user: RawlsUser)(
    testCode: (TestApiServiceWithCustomSamDAO) => T
  ) = {
    val apiService = new TestApiServiceWithCustomSamDAO(dataSource, user)(executionContext)

    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  behavior of "getSpendReportTableName"
  it should "return the correct fully formatted BigQuery table name if the spend report config is set" in withTestDataServices {
    services =>
      val billingProjectName = RawlsBillingProjectName("test-project")
      val billingProject = RawlsBillingProject(
        billingProjectName,
        CreationStatuses.Ready,
        None,
        None,
        None,
        None,
        None,
        false,
        Some(BigQueryDatasetName("bar")),
        Some(BigQueryTableName("baz")),
        Some(GoogleProject("foo"))
      )
      runAndWait(services.workspaceService.dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

      val result = Await.result(services.submissionsService.getSpendReportTableName(billingProjectName), Duration.Inf)

      result shouldBe Some("foo.bar.baz")
  }

  it should "return None if the spend report config is not set" in withTestDataServices { services =>
    val billingProjectName = RawlsBillingProjectName("test-project")
    val billingProject = RawlsBillingProject(billingProjectName,
                                             CreationStatuses.Ready,
                                             None,
                                             None,
                                             None,
                                             None,
                                             None,
                                             false,
                                             None,
                                             None,
                                             None
    )
    runAndWait(services.workspaceService.dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

    val result = Await.result(services.submissionsService.getSpendReportTableName(billingProjectName), Duration.Inf)

    result shouldBe None
  }

  it should "throw a RawlsExceptionWithErrorReport if the billing project does not exist" in withTestDataServices {
    services =>
      val billingProjectName = RawlsBillingProjectName("test-project")

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.submissionsService.getSpendReportTableName(billingProjectName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.NotFound
  }

  "getSubmissionMethodConfiguration" should "return the method configuration that was used to launch the submission" in withTestDataServices {
    services =>
      val workspaceName = testData.workspaceSuccessfulSubmission.toWorkspaceName
      val originalMethodConfig = testData.agoraMethodConfig

      // Overwrite the method configuration that was used for the submission. This forces it to generate a new version and soft-delete the old one
      Await.result(
        services.methodConfigurationService.overwriteMethodConfiguration(
          workspaceName,
          originalMethodConfig.namespace,
          originalMethodConfig.name,
          originalMethodConfig.copy(inputs = Map("i1" -> AttributeString("input_updated")))
        ),
        Duration.Inf
      )

      val firstSubmission =
        Await.result(services.submissionsService.listSubmissions(workspaceName, testContext), Duration.Inf).head

      val result = Await.result(
        services.submissionsService.getSubmissionMethodConfiguration(workspaceName, firstSubmission.submissionId),
        Duration.Inf
      )

      // None of the following attributes of a method config change when it is soft-deleted
      assertResult(originalMethodConfig.namespace)(result.namespace)
      assertResult(originalMethodConfig.inputs)(result.inputs)
      assertResult(originalMethodConfig.outputs)(result.outputs)
      assertResult(originalMethodConfig.prerequisites)(result.prerequisites)
      assertResult(originalMethodConfig.methodConfigVersion)(result.methodConfigVersion)
      assertResult(originalMethodConfig.methodRepoMethod)(result.methodRepoMethod)
      assertResult(originalMethodConfig.rootEntityType)(result.rootEntityType)

      // The following attributes are modified when it is soft-deleted
      assert(
        result.name.startsWith(originalMethodConfig.name)
      ) // a random suffix is added in this case, should be something like "testConfig1_HoQyHjLZ"
      assert(result.deleted)
      assert(result.deletedDate.isDefined)
  }

}
