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
import org.scalatest.{BeforeAndAfterAll, OptionValues}

import java.util.concurrent.TimeUnit
import java.time.{Duration => JavaDuration, OffsetDateTime}
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
    val fastPassServiceConstructor = FastPassService.constructor(
      fastPassConfig,
      googleIamDAO,
      googleStorageDAO,
      samDAO,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole
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

  "FastPassService" should "add FastPassGrants for the user on workspace create" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val beforeCreate = OffsetDateTime.now()
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

    val samUserStatus = Await.result(services.samDAO.getUserStatus(services.ctx1), Duration.Inf).orNull

    val ownerRoles = Vector(
      services.terraWorkspaceCanComputeRole,
      services.terraWorkspaceNextflowRole,
      services.terraBucketWriterRole
    )
    workspaceFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.organizationRole) should contain only (ownerRoles: _*)
    workspaceFastPassGrants.map(_.userSubjectId) should contain only (WorkbenchUserId(samUserStatus.userSubjectId))

    val userFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForUser(WorkbenchUserId(samUserStatus.userSubjectId)))
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

    val petEmail =
      Await.result(services.samDAO.getUserPetServiceAccount(services.ctx1, workspace.googleProjectId), Duration.Inf)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(samUserStatus.userEmail)))
    )

    // The user's pet is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(samUserStatus.userEmail)))
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(samUserStatus.userEmail))),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
    )

    // The user's pet is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(samUserStatus.userEmail))),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
    )
  }

  it should "remove FastPassGrants for the user on workspace delete" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val newWorkspaceName2 = "space_for_workin2"
    val workspaceRequest2 = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName2, Map.empty)

    val samUserStatus = Await.result(services.samDAO.getUserStatus(services.ctx1), Duration.Inf).orNull

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val workspace2 = Await.result(services.workspaceService.createWorkspace(workspaceRequest2), Duration.Inf)

    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))
    workspaceFastPassGrants should not be empty

    Await.ready(services.workspaceService.deleteWorkspace(workspaceRequest.toWorkspaceName), Duration.Inf)

    val noMoreWorkspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))
    noMoreWorkspaceFastPassGrants should be(empty)

    val yesMoreWorkspace2FastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace2.workspaceIdAsUUID))
    yesMoreWorkspace2FastPassGrants should not be empty

    val petEmail =
      Await.result(services.samDAO.getUserPetServiceAccount(services.ctx1, workspace.googleProjectId), Duration.Inf)

    // The user is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user's pet is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(WorkbenchEmail(samUserStatus.userEmail)),
      ArgumentMatchers.eq(IamMemberTypes.User),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
    )

    // The user's pet is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(petEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
    )
  }

  it should "add FastPassGrants for the user in the parent workspace bucket when a workspace is cloned" in withTestDataServices {
    services =>
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

      val parentWorkspaceFastPassGrantsAfter = runAndWait(
        fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(parentWorkspace.workspaceIdAsUUID,
                                                                WorkbenchUserId(samUserStatus.userSubjectId)
        )
      )

      val parentWorkspacePet = Await
        .result(services.samDAO.getUserPetServiceAccount(services.ctx1, parentWorkspace.googleProjectId), Duration.Inf)
        .value
      val childWorkspacePet = Await
        .result(services.samDAO.getUserPetServiceAccount(services.ctx1, childWorkspace.googleProjectId), Duration.Inf)
        .value

      parentWorkspaceFastPassGrantsAfter.map(_.accountEmail).toSet should be(
        Set(WorkbenchEmail(childWorkspacePet), WorkbenchEmail(samUserStatus.userEmail))
      )

  }

  it should "not do anything if its disabled in configs" in withTestDataServicesFastPassDisabled { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

    workspaceFastPassGrants should have size 0
  }

  it should "not do anything if there's no project IAM Policy binding quota available" in withTestDataServices {
    services =>
      val projectPolicy = toProjectPolicy(
        Policy(Range(0, FastPassService.policyBindingsQuotaLimit)
                 .map(i => Binding(s"role$i", Set("foo@bar.com"), null))
                 .toSet,
               "abcd"
        )
      )
      when(
        services.googleIamDAO.getProjectPolicy(any[GoogleProject])
      ).thenReturn(Future.successful(projectPolicy))

      val newWorkspaceName = "space_for_workin"
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      val workspaceFastPassGrants =
        runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

      workspaceFastPassGrants should have size 0
  }

  it should "not do anything if there's no bucket IAM Policy binding quota available" in withTestDataServices {
    services =>
      val bucketPolicy = toBucketPolicy(
        Policy(Range(0, FastPassService.policyBindingsQuotaLimit)
                 .map(i => Binding(s"role$i", Set("foo@bar.com"), null))
                 .toSet,
               "abcd"
        )
      )
      when(
        services.googleStorageDAO.getBucketPolicy(any[GcsBucketName], any[Option[GoogleProject]])
      ).thenReturn(Future.successful(bucketPolicy))

      val newWorkspaceName = "space_for_workin"
      val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
      val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
      val workspaceFastPassGrants =
        runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

      workspaceFastPassGrants should have size 0
  }

  it should "support service account users" in withTestDataServicesCustomUser(serviceAccountUser) { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    val workspaceFastPassGrants =
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

    val userEmail = WorkbenchEmail(services.user.userEmail.value)
    val petEmail =
      Await.result(services.samDAO.getUserPetServiceAccount(services.ctx1, workspace.googleProjectId), Duration.Inf)

    workspaceFastPassGrants should not be empty
    workspaceFastPassGrants.map(_.accountType) should contain only (IamMemberTypes.ServiceAccount)
    workspaceFastPassGrants.map(_.accountEmail) should contain only (userEmail, petEmail)

    // The user is added to the project IAM policies with a condition
    verify(services.googleIamDAO).addRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value)))
    )

    // The user is added to the bucket IAM policies with a condition
    verify(services.googleStorageDAO).addIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.argThat((c: Option[Expr]) => c.exists(_.title.contains(userEmail.value))),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
    )

    Await.ready(services.workspaceService.deleteWorkspace(workspaceRequest.toWorkspaceName), Duration.Inf)

    // The user is removed from the project IAM policies
    verify(services.googleIamDAO).removeRoles(
      ArgumentMatchers.eq(GoogleProject(workspace.googleProjectId.value)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraWorkspaceCanComputeRole, services.terraWorkspaceNextflowRole)),
      ArgumentMatchers.eq(false)
    )

    // The user is removed from the bucket IAM policies
    verify(services.googleStorageDAO).removeIamRoles(
      ArgumentMatchers.eq(GcsBucketName(workspace.bucketName)),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.eq(IamMemberTypes.ServiceAccount),
      ArgumentMatchers.eq(Set(services.terraBucketWriterRole)),
      ArgumentMatchers.eq(false),
      ArgumentMatchers.eq(Some(GoogleProject(workspace.googleProjectId.value)))
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

  it should "collect errors while removing FastPass grants" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    val fastPassGrants = runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))

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
                                                               workspace.googleProjectId,
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
      runAndWait(fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID))
    remainingFastPassGrants should not be empty
    remainingFastPassGrants should contain theSameElementsAs failedFastPassGrantRemovals

  }

  it should "not concurrently run IAM updates in a single Google Project" in withTestDataServicesSlowIam { services =>
    val expirationDate = OffsetDateTime.now().minus(JavaDuration.ofHours(3))
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
            OffsetDateTime.now()
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
            OffsetDateTime.now()
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
        OffsetDateTime.now()
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
        OffsetDateTime.now()
      )

      projectRoles ++ Seq(userBucketGrant, petBucketGrant)
    }

    allGrants.foreach(grant => runAndWait(fastPassGrantQuery.insert(grant)))

    val startTime = OffsetDateTime.now()

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

    val endTime = OffsetDateTime.now()

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
}
