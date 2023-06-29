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
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.HttpGoogleIamDAO.toProjectPolicy
import org.broadinstitute.dsde.workbench.google.HttpGoogleStorageDAO.toBucketPolicy
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.{Binding, Expr, IamMemberTypes, IamResourceTypes, Policy}
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, OneInstancePerTest, OptionValues}

import java.util.concurrent.TimeUnit
import java.time.{Duration => JavaDuration, LocalDateTime, OffsetDateTime, ZoneOffset}
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
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
    with OptionValues
    with OneInstancePerTest {
  import driver.api._

  val serviceAccountUser =
    testData.userOwner.copy(userEmail = RawlsUserEmail("service-account@project-name.iam.gserviceaccount.com"))

  val mockServer = RemoteServicesMockServer()

  val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  def slowFuture(slow: Boolean): Future[Boolean] =
    if (slow) {
      Future.successful {
        Thread.sleep(1000)
        true
      }
    } else {
      Future.successful(true)
    }

  class SlowGoogleIamDAO(val slow: Boolean) extends MockGoogleIamDAO {
    override def removeRoles(googleProject: GoogleProject,
                             userEmail: WorkbenchEmail,
                             memberType: IamMemberType,
                             rolesToRemove: Set[String],
                             retryIfGroupDoesNotExist: Boolean
    ): Future[Boolean] =
      slowFuture(slow)
  }

  class SlowGoogleStorageDAO(val slow: Boolean) extends MockGoogleStorageDAO {
    override def removeIamRoles(bucketName: GcsBucketName,
                                userEmail: WorkbenchEmail,
                                memberType: IamMemberType,
                                rolesToRemove: Set[String],
                                retryIfGroupDoesNotExist: Boolean,
                                userProject: Option[GoogleProject]
    ): Future[Boolean] =
      slowFuture(slow)
  }

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource,
                       val user: RawlsUser,
                       val fastPassEnabled: Boolean,
                       slowIam: Boolean = false
  )(implicit
    val executionContext: ExecutionContext
  ) extends WorkspaceApiService
      with MethodConfigApiService
      with SubmissionApiService
      with MockUserInfoDirectivesWithUser {
    val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    implicit val openTelemetry = FakeOpenTelemetryMetricsInterpreter

    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(ctx1)
    lazy val userService: UserService = userServiceConstructor(ctx1)
    val slickDataSource: SlickDataSource = dataSource

    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())
    val gcsDAO = Mockito.spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val googleIamDAO: MockGoogleIamDAO = Mockito.spy(new SlowGoogleIamDAO(slowIam))
    val googleStorageDAO: MockGoogleStorageDAO = Mockito.spy(new SlowGoogleStorageDAO(slowIam))
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
          SubmissionMonitorConfig(1 second, 30 days, true, 20000, true),
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
        leonardoDAO,
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

    val fastPassConfig = FastPassConfig.apply(testConf).copy(enabled = fastPassEnabled)
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

    val fastPassServiceConstructor = (_: RawlsRequestContext, _: SlickDataSource) => mockFastPassService

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

  def withTestDataServicesFastPassDisabled[T](testCode: TestApiService => T) =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner, fastPassEnabled = false)(testCode)
    }

  def withTestDataServicesSlowIam[T](testCode: TestApiService => T) =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner, slowIam = true)(testCode)
    }

  def withTestDataServicesCustomUser[T](user: RawlsUser)(testCode: TestApiService => T) =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, user)(testCode)
    }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  def withServices[T](dataSource: SlickDataSource,
                      user: RawlsUser,
                      fastPassEnabled: Boolean = true,
                      slowIam: Boolean = false
  )(
    testCode: (TestApiService) => T
  ) = {
    val apiService = new TestApiService(dataSource, user, fastPassEnabled, slowIam)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  "FastPassService" should "be called on workspace create" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    verify(services.mockFastPassService)
      .syncFastPassesForUserInWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(workspace.workspaceId)),
        ArgumentMatchers.eq(services.user.userEmail.value)
      )
  }

  it should "be called when ACLs are updated" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val aclAdd = Set(
      WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, canCompute = Option(true)),
      WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, canShare = Option(true))
    )
    Await.ready(services.workspaceService.updateACL(workspace.toWorkspaceName, aclAdd, false), Duration.Inf)

    verify(services.mockFastPassService)
      .syncFastPassesForUserInWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(workspace.workspaceId)),
        ArgumentMatchers.eq(testData.userWriter.userEmail.value)
      )

    verify(services.mockFastPassService)
      .syncFastPassesForUserInWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(workspace.workspaceId)),
        ArgumentMatchers.eq(testData.userReader.userEmail.value)
      )
  }

  it should "be called on workspace clone" in withTestDataServices { services =>
    val parentWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val samUserStatus = Await.result(services.samDAO.getUserStatus(services.ctx1), Duration.Inf).orNull
    val parentWorkspaceFastPassGrantsBefore = runAndWait(
      fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(parentWorkspace.workspaceIdAsUUID,
                                                              WorkbenchUserId(samUserStatus.userSubjectId)
      )
    )

    parentWorkspaceFastPassGrantsBefore should be(empty)

    val childWorkspace =
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
                                                                        parentWorkspace.toWorkspaceName,
                                                                        workspaceRequest
                   ),
                   Duration.Inf
      )

    verify(services.mockFastPassService)
      .setupFastPassForUserInClonedWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(parentWorkspace.workspaceId)),
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(childWorkspace.workspaceId))
      )

    verify(services.mockFastPassService)
      .syncFastPassesForUserInWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(childWorkspace.workspaceId)),
        ArgumentMatchers.eq(services.user.userEmail.value)
      )
  }

  it should "be called on workspace delete" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    Await.ready(services.workspaceService.deleteWorkspace(workspaceRequest.toWorkspaceName), Duration.Inf)

    verify(services.mockFastPassService)
      .removeFastPassGrantsForWorkspace(
        ArgumentMatchers.argThat((w: Workspace) => w.workspaceId.equals(workspace.workspaceId))
      )
  }

  it should "sync FastPass grants for a workspace with the current user context" in withTestDataServices { services =>
    val beforeCreate = OffsetDateTime.now(ZoneOffset.UTC)
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    val samUserStatus = Await
      .result(services.fastPassMockSamDAO.getUserIdInfoForEmail(WorkbenchEmail(services.user.userEmail.value)),
              Duration.Inf
      )
    val userSubjectId = WorkbenchUserId(samUserStatus.userSubjectId)
    val userEmail = WorkbenchEmail(samUserStatus.userEmail)

    val ownerRoles = Vector(
      services.terraWorkspaceCanComputeRole,
      services.terraWorkspaceNextflowRole,
      services.terraBucketWriterRole
    )
    workspaceFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)
    workspaceFastPassGrants.map(_.userSubjectId) should contain only userSubjectId

    val userFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForUser(userSubjectId))
    userFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)

    val userAccountFastPassGrants = userFastPassGrants.filter(_.accountType.equals(IamMemberTypes.User))
    val petAccountFastPassGrants = userFastPassGrants.filter(_.accountType.equals(IamMemberTypes.ServiceAccount))
    userAccountFastPassGrants.length should be(petAccountFastPassGrants.length)

    val userResourceRoles =
      userAccountFastPassGrants.map(g => (g.resourceType, g.resourceName, g.organizationRole)).toSet
    val petResourceRoles = petAccountFastPassGrants.map(g => (g.resourceType, g.resourceName, g.organizationRole)).toSet
    userResourceRoles should be(petResourceRoles)

    val bucketGrant = userFastPassGrants.find(_.resourceType == IamResourceTypes.Bucket).get
    val timeBetween = JavaDuration.between(beforeCreate, bucketGrant.expiration)
    timeBetween.toHoursPart should be(services.fastPassConfig.grantPeriod.toHoursPart)

    val petKey =
      Await.result(
        services.fastPassMockSamDAO.getPetServiceAccountKeyForUser(testData.workspace.googleProjectId,
                                                                   RawlsUserEmail(samUserStatus.userEmail)
        ),
        Duration.Inf
      )
    val petEmail = FastPassService.getEmailFromPetSaKey(petKey)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value)))
    )

    // The user's pet is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value)))
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value))),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    // The user's pet is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value))),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )
  }

  it should "remove FastPass grants for a workspace" in withTestDataServices { services =>
    val samUserStatus = Await.result(services.fastPassMockSamDAO.getUserStatus(services.ctx1), Duration.Inf).orNull

    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspaceNoAttrs), Duration.Inf)

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    workspaceFastPassGrants should not be empty

    Await.ready(services.mockFastPassService.removeFastPassGrantsForWorkspace(testData.workspace), Duration.Inf)

    val noMoreWorkspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    noMoreWorkspaceFastPassGrants should be(empty)

    val yesMoreWorkspace2FastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspaceNoAttrs.workspaceIdAsUUID))
    yesMoreWorkspace2FastPassGrants should not be empty

    val petKey =
      Await.result(
        services.fastPassMockSamDAO.getPetServiceAccountKeyForUser(testData.workspace.googleProjectId,
                                                                   RawlsUserEmail(samUserStatus.userEmail)
        ),
        Duration.Inf
      )
    val petEmail = FastPassService.getEmailFromPetSaKey(petKey)

    // The user is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user's pet is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    // The user's pet is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )
  }

  it should "add FastPass grants for the user in the parent workspace bucket when a workspace is cloned" in withTestDataServices {
    services =>
      val parentWorkspace = testData.workspace
      val childWorkspace = testData.workspaceNoAttrs

      val samUserStatus = Await.result(services.fastPassMockSamDAO.getUserStatus(services.ctx1), Duration.Inf).orNull
      val parentWorkspaceFastPassGrantsBefore = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(parentWorkspace.workspaceIdAsUUID,
                                                                WorkbenchUserId(samUserStatus.userSubjectId)
        )
      )

      parentWorkspaceFastPassGrantsBefore should be(empty)

      Await.ready(services.mockFastPassService.setupFastPassForUserInClonedWorkspace(parentWorkspace, childWorkspace),
                  Duration.Inf
      )

      val parentWorkspaceFastPassGrantsAfter = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(parentWorkspace.workspaceIdAsUUID,
                                                                WorkbenchUserId(samUserStatus.userSubjectId)
        )
      )

      val parentWorkspacePetKey =
        Await.result(
          services.fastPassMockSamDAO.getPetServiceAccountKeyForUser(parentWorkspace.googleProjectId,
                                                                     RawlsUserEmail(samUserStatus.userEmail)
          ),
          Duration.Inf
        )
      val parentWorkspacePetEmail = FastPassService.getEmailFromPetSaKey(parentWorkspacePetKey)

      val childWorkspacePetKey =
        Await.result(
          services.fastPassMockSamDAO.getPetServiceAccountKeyForUser(childWorkspace.googleProjectId,
                                                                     RawlsUserEmail(samUserStatus.userEmail)
          ),
          Duration.Inf
        )
      val childWorkspacePetEmail = FastPassService.getEmailFromPetSaKey(childWorkspacePetKey)

      parentWorkspacePetEmail should not be childWorkspacePetEmail

      parentWorkspaceFastPassGrantsAfter.map(_.accountEmail).toSet should be(
        Set(childWorkspacePetEmail, WorkbenchEmail(samUserStatus.userEmail))
      )

  }

  it should "only add one bucket FastPass grant when a user clones a workspace multiple times" in withTestDataServices {
    services =>
      val parentWorkspace = testData.workspacePublished
      val childWorkspace = testData.workspace
      val childWorkspace2 = testData.workspaceNoAttrs

      val samUserStatus = Await.result(services.fastPassMockSamDAO.getUserStatus(services.ctx1), Duration.Inf).orNull

      Await.result(services.mockFastPassService.setupFastPassForUserInClonedWorkspace(parentWorkspace, childWorkspace),
                   Duration.Inf
      )
      Await.result(services.mockFastPassService.setupFastPassForUserInClonedWorkspace(parentWorkspace, childWorkspace2),
                   Duration.Inf
      )

      val parentWorkspaceFastPassGrantsAfter = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(parentWorkspace.workspaceIdAsUUID,
                                                                WorkbenchUserId(samUserStatus.userSubjectId)
        )
      )

      // There should only be 1 grant even though 2 workspaces were cloned
      parentWorkspaceFastPassGrantsAfter.filter(g => g.accountType.equals(IamMemberTypes.User)) should have size 1
  }

  it should "sync FastPass grants when users ACLs are modified" in withTestDataServices { services =>
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                              testData.userReader.userEmail.value
                ),
                Duration.Inf
    )
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                              testData.userWriter.userEmail.value
                ),
                Duration.Inf
    )

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    val userWriterGrants =
      workspaceFastPassGrants.filter(fpg => fpg.userSubjectId.value == testData.userWriter.userSubjectId.value)
    val userReaderGrants =
      workspaceFastPassGrants.filter(fpg => fpg.userSubjectId.value == testData.userReader.userSubjectId.value)

    val writerCanComputeRoles = Vector(
      services.terraWorkspaceCanComputeRole,
      services.terraWorkspaceNextflowRole,
      services.terraBucketWriterRole
    )
    val readerRoles = Vector(services.terraBucketReaderRole)

    userWriterGrants.map(_.organizationRole) should contain only (writerCanComputeRoles: _*)
    userReaderGrants.map(_.organizationRole) should contain only (readerRoles: _*)

    // share-reader added as bucket reader
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(testData.userReader.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketReaderRole)),
      ArgumentMatchers.anyBoolean(),
      ArgumentMatchers.any[Option[Expr]],
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    // writer added with project roles
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(testData.userWriter.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceNextflowRole, services.terraWorkspaceCanComputeRole)),
      ArgumentMatchers.anyBoolean(),
      ArgumentMatchers.any[Option[Expr]]
    )

    // writer added as bucket writer
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(testData.userWriter.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.anyBoolean(),
      ArgumentMatchers.any[Option[Expr]],
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    // Make userWriter have no access to the workspace
    doReturn(
      Future.successful(Set.empty)
    ).when(services.fastPassMockSamDAO)
      .listUserRolesForResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(testData.workspace.workspaceId),
        ArgumentMatchers.any[RawlsRequestContext]
      )

    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                              testData.userWriter.userEmail.value
                ),
                Duration.Inf
    )

    // writer removed from project roles
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(testData.userWriter.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceNextflowRole, services.terraWorkspaceCanComputeRole)),
      ArgumentMatchers.anyBoolean()
    )

    // writer removed as bucket writer
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(testData.userWriter.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.anyBoolean(),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    val workspaceFastPassGrantsAfter =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    val userWriterGrantsAfter =
      workspaceFastPassGrantsAfter.filter(fpg => fpg.userSubjectId.value == testData.userWriter.userSubjectId.value)
    userWriterGrantsAfter should be(empty)
  }

  it should "not add FastPass grants for groups" in withTestDataServices { services =>
    // Make group have  access to the workspace
    doReturn(
      Future.successful(Set(SamWorkspaceRoles.shareReader))
    ).when(services.fastPassMockSamDAO)
      .listUserRolesForResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(testData.workspace.workspaceId),
        ArgumentMatchers.any[RawlsRequestContext]
      )

    doReturn(
      Future.successful(SamDAO.NotUser)
    ).when(services.fastPassMockSamDAO)
      .getUserIdInfo(
        ArgumentMatchers.eq(testData.nestedProjectGroup.groupEmail.value),
        ArgumentMatchers.any[RawlsRequestContext]
      )

    Await.ready(services.mockFastPassService
                  .syncFastPassesForUserInWorkspace(testData.workspace, testData.nestedProjectGroup.groupEmail.value),
                Duration.Inf
    )

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    workspaceFastPassGrants should be(empty)
  }

  it should "not add FastPass grants for 'not found' users" in withTestDataServices { services =>
    doReturn(
      Future.successful(SamDAO.NotFound)
    ).when(services.fastPassMockSamDAO)
      .getUserIdInfo(
        ArgumentMatchers.eq(testData.userReader.userEmail.value),
        ArgumentMatchers.any[RawlsRequestContext]
      )
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                              testData.userReader.userEmail.value
                ),
                Duration.Inf
    )

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    workspaceFastPassGrants should be(empty)
  }

  it should "not do anything if its disabled in configs" in withTestDataServicesFastPassDisabled { services =>
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                              testData.userWriter.userEmail.value
                ),
                Duration.Inf
    )
    Await.ready(
      services.mockFastPassService.setupFastPassForUserInClonedWorkspace(testData.workspace, testData.workspaceNoAttrs),
      Duration.Inf
    )

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    workspaceFastPassGrants should have size 0
  }

  it should "not do anything if there's no project IAM Policy binding quota available" in withTestDataServices {
    services =>
      val projectPolicy = toProjectPolicy(
        Policy(Range(0, FastPassService.policyBindingsQuotaLimit - 1)
                 .map(i => Binding(s"role$i", Set("foo@bar.com"), null))
                 .toSet,
               "abcd"
        )
      )
      when(
        services.googleIamDAO.getProjectPolicy(any[GoogleProject])
      ).thenReturn(Future.successful(projectPolicy))

      Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                                testData.userWriter.userEmail.value
                  ),
                  Duration.Inf
      )
      val workspaceFastPassGrants =
        runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

      workspaceFastPassGrants should have size 0
  }

  it should "not do anything if there's no bucket IAM Policy binding quota available" in withTestDataServices {
    services =>
      val bucketPolicy = toBucketPolicy(
        Policy(Range(0, FastPassService.policyBindingsQuotaLimit - 1)
                 .map(i => Binding(s"role$i", Set("foo@bar.com"), null))
                 .toSet,
               "abcd"
        )
      )
      when(
        services.googleStorageDAO.getBucketPolicy(any[GcsBucketName], any[Option[GoogleProject]])
      ).thenReturn(Future.successful(bucketPolicy))

      Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace,
                                                                                testData.userWriter.userEmail.value
                  ),
                  Duration.Inf
      )
      val workspaceFastPassGrants =
        runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

      workspaceFastPassGrants should have size 0
  }

  it should "support service account users" in withTestDataServicesCustomUser(serviceAccountUser) { services =>
    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    val userEmail = WorkbenchEmail(services.user.userEmail.value)
    val petKey =
      Await.result(
        services.fastPassMockSamDAO.getPetServiceAccountKeyForUser(testData.workspace.googleProjectId,
                                                                   services.user.userEmail
        ),
        Duration.Inf
      )
    val petEmail = FastPassService.getEmailFromPetSaKey(petKey)

    workspaceFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.accountType) should contain only (IamMemberTypes.ServiceAccount)
    workspaceFastPassGrants.map(_.accountEmail) should contain only (userEmail, petEmail)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value)))
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value))),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    Await.ready(services.mockFastPassService.removeFastPassGrantsForWorkspace(testData.workspace), Duration.Inf)

    // The user is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )
  }

  it should "not block workspace creation if FastPass fails" in withTestDataServices { services =>
    doThrow(new RuntimeException("foo"))
      .when(services.googleIamDAO)
      .getProjectPolicy(ArgumentMatchers.any[GoogleProject])

    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
  }

  it should "not block workspace cloning if FastPass fails" in withTestDataServices { services =>
    doThrow(new RuntimeException("foo"))
      .when(services.fastPassMockSamDAO)
      .getUserPetServiceAccount(ArgumentMatchers.any[RawlsRequestContext], ArgumentMatchers.any[GoogleProjectId])
    val parentWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val childWorkspace =
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
                                                                        parentWorkspace.toWorkspaceName,
                                                                        workspaceRequest
                   ),
                   Duration.Inf
      )
  }

  it should "not block workspace delete if FastPass fails" in withTestDataServices { services =>
    doThrow(new RuntimeException("foo"))
      .when(services.googleStorageDAO)
      .removeIamRoles(
        ArgumentMatchers.any[GcsBucketName],
        ArgumentMatchers.any[WorkbenchEmail],
        ArgumentMatchers.any[IamMemberType],
        ArgumentMatchers.any[Set[String]],
        ArgumentMatchers.any[Boolean],
        any[Option[GoogleProject]]
      )
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    Await.ready(services.workspaceService.deleteWorkspace(workspaceRequest.toWorkspaceName), Duration.Inf)
  }

  it should "not block workspace ACL modifications if FastPass fails" in withTestDataServices { services =>
    doThrow(new RuntimeException("foo"))
      .when(services.googleStorageDAO)
      .addIamRoles(
        ArgumentMatchers.any[GcsBucketName],
        ArgumentMatchers.any[WorkbenchEmail],
        ArgumentMatchers.any[IamMemberType],
        ArgumentMatchers.any[Set[String]],
        ArgumentMatchers.any[Boolean],
        ArgumentMatchers.any[Option[Expr]],
        any[Option[GoogleProject]]
      )
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val aclAdd = Set(
      WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Write, canCompute = Option(true)),
      WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, canShare = Option(true))
    )
    Await.ready(services.workspaceService.updateACL(workspace.toWorkspaceName, aclAdd, false), Duration.Inf)
  }

  it should "collect errors while removing FastPass grants" in withTestDataServices { services =>
    Await.result(
      services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace, services.user.userEmail.value),
      Duration.Inf
    )

    val fastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    when(
      services.googleStorageDAO.removeIamRoles(
        any[GcsBucketName],
        ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
        any[IamMemberType],
        any[Set[String]],
        any[Boolean],
        any[Option[GoogleProject]]
      )
    ).thenReturn(Future.failed(new RawlsException("TEST FAILURE")))

    val failedRemovals = Await.result(
      slickDataSource.inTransaction { dataAccess =>
        FastPassService.removeFastPassGrantsInWorkspaceProject(fastPassGrants,
                                                               testData.workspace.googleProjectId,
                                                               dataAccess,
                                                               services.googleIamDAO,
                                                               services.googleStorageDAO,
                                                               None
        )(executionContext, services.openTelemetry)
      },
      Duration.Inf
    )

    val failedFastPassGrantRemovals = failedRemovals.flatMap(_._2)

    failedFastPassGrantRemovals.map(_.resourceType) should contain only (IamResourceTypes.Bucket)
    failedFastPassGrantRemovals.map(_.accountEmail.value) should contain only (services.user.userEmail.value)

    val remainingFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    remainingFastPassGrants should not be empty
    remainingFastPassGrants should contain theSameElementsAs failedFastPassGrantRemovals

  }

  it should "not leave FastPass grants around if one fails" in withTestDataServices { services =>
    doThrow(new RuntimeException("foo"))
      .when(services.googleStorageDAO)
      .addIamRoles(
        ArgumentMatchers.any[GcsBucketName],
        ArgumentMatchers.any[WorkbenchEmail],
        ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
        ArgumentMatchers.any[Set[String]],
        ArgumentMatchers.any[Boolean],
        ArgumentMatchers.any[Option[Expr]],
        ArgumentMatchers.any[Option[GoogleProject]]
      )

    val foo = Await.result(
      services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace, services.user.userEmail.value),
      Duration.Inf
    )

    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.any[Set[String]],
      ArgumentMatchers.any[Boolean]
    )

    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(testData.workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.any[Set[String]],
      ArgumentMatchers.any[Boolean]
    )

    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(testData.workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(services.user.userEmail.value)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.any[Set[String]],
      ArgumentMatchers.any[Boolean],
      ArgumentMatchers.eq(Some(GoogleProject(testData.workspace.googleProjectId.value)))
    )

    val grants = runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    grants shouldBe empty

  }

  it should "not concurrently run IAM updates in a single Google Project" in withTestDataServicesSlowIam { services =>
    val expirationDate = OffsetDateTime.now(ZoneOffset.UTC).minus(JavaDuration.ofHours(3))
    val allGrants = Seq(testData.workspace, testData.workspaceNoAttrs).flatMap { workspace =>
      val projectRoles = Seq(services.terraWorkspaceNextflowRole, services.terraWorkspaceCanComputeRole).flatMap {
        role =>
          val userGrant = FastPassGrant(
            0,
            workspace.workspaceId,
            WorkbenchUserId(testData.userOwner.userSubjectId.value),
            WorkbenchEmail(testData.userOwner.userEmail.value),
            IamMemberTypes.User,
            IamResourceTypes.Project,
            workspace.googleProjectId.value,
            role,
            expirationDate,
            OffsetDateTime.now(ZoneOffset.UTC)
          )
          val petGrant = FastPassGrant(
            0,
            workspace.workspaceId,
            WorkbenchUserId(testData.userOwner.userSubjectId.value),
            WorkbenchEmail(testData.userOwner.userEmail.value + "-pet"),
            IamMemberTypes.ServiceAccount,
            IamResourceTypes.Project,
            workspace.googleProjectId.value,
            role,
            expirationDate,
            OffsetDateTime.now(ZoneOffset.UTC)
          )
          Seq(userGrant, petGrant)
      }

      val userBucketGrant = FastPassGrant(
        0,
        workspace.workspaceId,
        WorkbenchUserId(testData.userOwner.userSubjectId.value),
        WorkbenchEmail(testData.userOwner.userEmail.value),
        IamMemberTypes.User,
        IamResourceTypes.Bucket,
        workspace.bucketName.value,
        services.terraBucketWriterRole,
        expirationDate,
        OffsetDateTime.now(ZoneOffset.UTC)
      )

      val petBucketGrant = FastPassGrant(
        0,
        workspace.workspaceId,
        WorkbenchUserId(testData.userOwner.userSubjectId.value),
        WorkbenchEmail(testData.userOwner.userEmail.value + "-pet"),
        IamMemberTypes.ServiceAccount,
        IamResourceTypes.Bucket,
        workspace.bucketName.value,
        services.terraBucketWriterRole,
        expirationDate,
        OffsetDateTime.now(ZoneOffset.UTC)
      )

      projectRoles ++ Seq(userBucketGrant, petBucketGrant)
    }

    allGrants.foreach(grant => runAndWait(fastPassGrantQuery.insert(grant)))

    val startTime = LocalDateTime.now()

    val fastPassGrants1 =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    val fastPassGrants2 =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspaceNoAttrs.workspaceIdAsUUID))

    val project1Removal =
      FastPassService.removeFastPassGrantsInWorkspaceProject(fastPassGrants1,
                                                             testData.workspace.googleProjectId,
                                                             slickDataSource.dataAccess,
                                                             services.googleIamDAO,
                                                             services.googleStorageDAO,
                                                             None
      )(executionContext, services.openTelemetry)
    val project2Removal =
      FastPassService.removeFastPassGrantsInWorkspaceProject(fastPassGrants2,
                                                             testData.workspaceNoAttrs.googleProjectId,
                                                             slickDataSource.dataAccess,
                                                             services.googleIamDAO,
                                                             services.googleStorageDAO,
                                                             None
      )(executionContext, services.openTelemetry)

    runAndWait(DBIO.seq(project1Removal, project2Removal))

    val endTime = LocalDateTime.now()

    val postCleanupWorkspace1FastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))
    val postCleanupWorkspace2FastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspaceNoAttrs.workspaceIdAsUUID))

    postCleanupWorkspace1FastPassGrants should be(empty)
    postCleanupWorkspace2FastPassGrants should be(empty)

    val timeBetween = JavaDuration.between(startTime, endTime)
    val seconds = timeBetween.toSecondsPart

    // Each IAM update takes 1 second.
    // Per workspace, there are 2 project calls and 2 bucket calls (user and pet).
    // Updates within a workspace happen serially, but two workspaces can be updated in parallel.
    // So, the updates should take 4 seconds. We allow for a range of 3-5 seconds to take hardware speed into account.
    // A value less than 3 means all updates ran concurrently when they shouldn't have.
    // A value more than 5 means the separate workspaces were updated sequentially, which is also incorrect behavior.
    seconds should be > 2
    seconds should be < 6
  }

  it should "not add FastPass grants for disabled users" in withTestDataServices { services =>
    val userStatusResponse = Some(
      SamUserStatusResponse(services.user.userSubjectId.value, services.user.userEmail.value, enabled = false)
    )
    doReturn(Future.successful(userStatusResponse))
      .when(services.fastPassMockSamDAO)
      .getUserStatus(
        ArgumentMatchers.argThat((ctx: RawlsRequestContext) =>
          ctx.userInfo.userSubjectId.value.startsWith(s"${services.user.userSubjectId.value}-pet")
        )
      )

    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    val fastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    fastPassGrants should be(empty)
  }

  it should "not add FastPass grants for not found users" in withTestDataServices { services =>
    doReturn(
      Future.successful(
        SamDAO.NotFound
      )
    ).when(services.fastPassMockSamDAO)
      .getUserIdInfo(
        ArgumentMatchers.eq(services.user.userEmail.value),
        ArgumentMatchers.any[RawlsRequestContext]
      )

    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    val fastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    fastPassGrants should be(empty)
  }

  it should "not add FastPass grants for non user-type users" in withTestDataServices { services =>
    doReturn(
      Future.successful(
        SamDAO.NotUser
      )
    ).when(services.fastPassMockSamDAO)
      .getUserIdInfo(
        ArgumentMatchers.eq(services.user.userEmail.value),
        ArgumentMatchers.any[RawlsRequestContext]
      )

    Await.ready(services.mockFastPassService.syncFastPassesForUserInWorkspace(testData.workspace), Duration.Inf)
    val fastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(testData.workspace.workspaceIdAsUUID))

    fastPassGrants should be(empty)
  }
}
