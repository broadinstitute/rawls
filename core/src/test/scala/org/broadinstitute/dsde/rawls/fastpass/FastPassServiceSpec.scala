package org.broadinstitute.dsde.rawls.fastpass

import akka.actor.PoisonPill
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAOImpl
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.{
  MultiCloudWorkspaceAclManager,
  MultiCloudWorkspaceService,
  RawlsWorkspaceAclManager,
  WorkspaceService
}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.IamModel.Expr
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

//noinspection NameBooleanParameters,TypeAnnotation,EmptyParenMethodAccessedAsParameterless,ScalaUnnecessaryParentheses,RedundantNewCaseClass,ScalaUnusedSymbol
class FastPassServiceSpec
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

  val workspace = Workspace(
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

  val mockServer = RemoteServicesMockServer()

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
    val executionContext: ExecutionContext
  ) extends WorkspaceApiService
      with MethodConfigApiService
      with SubmissionApiService
      with MockUserInfoDirectivesWithUser {
    val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(ctx1)
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
    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO(mockServer.mockServerBaseUrl)

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
          mockNotificationDAO,
          gcsDAO.getBucketServiceAccountCredential,
          SubmissionMonitorConfig(1 second, true, 20000, true),
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
    val servicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
    when(servicePerimeterService.overwriteGoogleProjectsInPerimeter(any[ServicePerimeterName], any[DataAccess]))
      .thenReturn(DBIO.successful(()))

    val billingProfileManagerDAO = mock[BillingProfileManagerDAOImpl](RETURNS_SMART_NULLS)

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      samDAO,
      MockBigQueryServiceFactory.ioFactory(),
      testConf.getString("gcs.pathToCredentialJson"),
      "requesterPaysRole",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate")),
      servicePerimeterService,
      RawlsBillingAccountName("billingAccounts/ABCDE-FGHIJ-KLMNO"),
      billingProfileManagerDAO,
      mock[WorkspaceManagerDAO],
      mock[NotificationDAO]
    ) _

    val genomicsServiceConstructor = GenomicsService.constructor(
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
        workbenchMetricBaseName
      )
    lazy val mcWorkspaceService: MultiCloudWorkspaceService = multiCloudWorkspaceServiceConstructor(ctx1)

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService =
      new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()
    val entityManager = EntityManager.defaultEntityManager(
      dataSource,
      workspaceManagerDAO,
      dataRepoDAO,
      samDAO,
      bigQueryServiceFactory,
      DataRepoEntityProviderConfig(100, 10, 0),
      testConf.getBoolean("entityStatisticsCache.enabled"),
      workbenchMetricBaseName
    )

    val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
    val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
    val resourceBufferService = Mockito.spy(new ResourceBufferService(resourceBufferDAO, resourceBufferConfig))
    val resourceBufferSaEmail = resourceBufferConfig.saEmail

    val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)

    val terraBillingProjectOwnerRole = "fakeTerraBillingProjectOwnerRole"
    val terraWorkspaceCanComputeRole = "fakeTerraWorkspaceCanComputeRole"
    val terraWorkspaceNextflowRole = "fakeTerraWorkspaceNextflowRole"
    val terraBucketReaderRole = "fakeTerraBucketReaderRole"
    val terraBucketWriterRole = "fakeTerraBucketWriterRole"

    val fastPassServiceConstructor = FastPassService.constructor(
      googleIamDAO,
      googleStorageDAO,
      samDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      workbenchMetricBaseName
    ) _

    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(
        MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
        MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
        workbenchMetricBaseName = workbenchMetricBaseName
      ),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
      execServiceBatchSize,
      workspaceManagerDAO,
      methodConfigResolver,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      genomicsServiceConstructor,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName,
      submissionCostService,
      workspaceServiceConfig,
      requesterPaysSetupService,
      entityManager,
      resourceBufferService,
      resourceBufferSaEmail,
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

    def cleanupSupervisor =
      submissionSupervisor ! PoisonPill
  }

  class TestApiServiceWithCustomSamDAO(dataSource: SlickDataSource, override val user: RawlsUser)(implicit
    override val executionContext: ExecutionContext
  ) extends TestApiService(dataSource, user) {
    override val samDAO: CustomizableMockSamDAO = Mockito.spy(new CustomizableMockSamDAO(dataSource))

    // these need to be overridden to use the new samDAO
    override val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    override val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  "FastPassService" should "add FastPassGrants for the user on workspace create" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

    val ownerRoles = Vector(
      services.terraWorkspaceCanComputeRole,
      services.terraWorkspaceNextflowRole,
      services.terraBucketWriterRole
    )
    workspaceFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)
    workspaceFastPassGrants.map(_.userSubjectId) should contain only (services.user.userSubjectId)

    val userFastPassGrants = runAndWait(fastPassGrantQuery.findFastPassGrantsForUser(services.user.userSubjectId))
    userFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)

    val petEmail =
      Await.result(services.samDAO.getUserPetServiceAccount(services.ctx1, workspace.googleProjectId), Duration.Inf)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addIamRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(MemberType.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(services.user.userEmail.value)))
    )

    // The user's pet is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addIamRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(MemberType.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(services.user.userEmail.value)))
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(MemberType.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(services.user.userEmail.value)))
    )

    // The user's pet is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(MemberType.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(services.user.userEmail.value)))
    )
  }

  it should "remove FastPassGrants for the user on workspace delete" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))
    workspaceFastPassGrants should not be empty

    Await.ready(services.workspaceService.deleteWorkspace(workspaceRequest.toWorkspaceName), Duration.Inf)

    val noMoreWorkspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))
    noMoreWorkspaceFastPassGrants should be(empty)

    val petEmail =
      Await.result(services.samDAO.getUserPetServiceAccount(services.ctx1, workspace.googleProjectId), Duration.Inf)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).removeIamRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(MemberType.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user's pet is added to the project IAM policies with a condition
    verify(services.googleIamDAO).removeIamRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(MemberType.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(MemberType.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false)
    )

    // The user's pet is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(MemberType.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false)
    )
  }

  it should "add FastPassGrants for the user in the parent workspace when a workspace is cloned" in withTestDataServices {
    services =>
      val baseWorkspace = testData.workspace
      val newWorkspaceName = "cloned_space"
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

      val baseWorkspaceFastPassGrantsBefore = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(baseWorkspace.workspaceIdAsUUID,
                                                                services.user.userSubjectId
        )
      )

      baseWorkspaceFastPassGrantsBefore should be(empty)

      val workspace =
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
                                                                          baseWorkspace.toWorkspaceName,
                                                                          workspaceRequest
                     ),
                     Duration.Inf
        )

      val baseWorkspaceFastPassGrantsAfter = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(baseWorkspace.workspaceIdAsUUID,
                                                                services.user.userSubjectId
        )
      )

      baseWorkspaceFastPassGrantsAfter should not be empty
  }
}
