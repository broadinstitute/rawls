package org.broadinstitute.dsde.rawls.workspace

import akka.actor.PoisonPill
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{AzureContext, GcpContext, WorkspaceDescription, WsmPolicyInput, WsmPolicyPair}
import cats.implicits.{catsSyntaxOptionId}
import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.iam.v1.model.Role
import com.google.cloud.storage.StorageException
import com.typesafe.config.ConfigFactory
import io.opencensus.trace.{Span => OpenCensusSpan}
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAOImpl
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.{
  NoSuchWorkspaceException,
  RawlsExceptionWithErrorReport,
  RawlsTestUtils
}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchGroupName}
import org.broadinstitute.dsde.workbench.model.google.{
  BigQueryDatasetName,
  BigQueryTableName,
  GcsBucketName,
  GoogleProject,
  IamPermission
}
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
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
import scala.language.postfixOps
import scala.util.Try
import scala.collection.JavaConverters._

//noinspection NameBooleanParameters,TypeAnnotation,EmptyParenMethodAccessedAsParameterless,ScalaUnnecessaryParentheses,RedundantNewCaseClass,ScalaUnusedSymbol
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

    val fastPassConfig = FastPassConfig.apply(testConf)
    val fastPassServiceConstructor = FastPassService.constructor(
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

  def withTestDataServicesCustomSamAndUser[T](user: RawlsUser)(testCode: TestApiServiceWithCustomSamDAO => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServicesCustomSam(dataSource, user)(testCode)
    }

  def withTestDataServicesCustomSam[T](testCode: TestApiServiceWithCustomSamDAO => T): T =
    withTestDataServicesCustomSamAndUser(testData.userOwner)(testCode)

  def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  private def withServicesCustomSam[T](dataSource: SlickDataSource, user: RawlsUser)(
    testCode: (TestApiServiceWithCustomSamDAO) => T
  ) = {
    val apiService = new TestApiServiceWithCustomSamDAO(dataSource, user)

    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

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
  private def populateWorkspacePolicies(services: TestApiService, workspace: Workspace = testData.workspace) = {
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
          Set(SamWorkspaceActions.own, SamWorkspaceActions.write, SamWorkspaceActions.read),
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

  it should "add ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val user2 = RawlsUser(RawlsUserSubjectId("obamaiscool2"), RawlsUserEmail("obama2@whitehouse.gov"))

    Await.result(for {
                   _ <- services.samDAO.registerUser(toRawlsRequestContext(user1))
                   _ <- services.samDAO.registerUser(toRawlsRequestContext(user2))
                 } yield (),
                 Duration.Inf
    )

    // add ACL
    val aclAdd = Set(
      WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, None),
      WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Option(true))
    )
    val aclAddResponse =
      Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclAdd, false), Duration.Inf)
    val responseFromAdd = WorkspaceACLUpdateResponseList(
      Set(
        WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, Some(true), Some(true)),
        WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Some(true), Some(false))
      ),
      Set.empty,
      Set.empty
    )

    assertResult(responseFromAdd, aclAddResponse.toString) {
      aclAddResponse
    }

    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.owner,
       user1.userEmail.value
      ),
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.shareReader,
       user2.userEmail.value
      ),
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.reader,
       user2.userEmail.value
      )
    )
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq.empty
  }

  it should "update ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    // update ACL
    val aclUpdates = Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None))
    val aclUpdateResponse =
      Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false),
                   Duration.Inf
      )
    val responseFromUpdate = WorkspaceACLUpdateResponseList(
      Set(
        WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, Some(false), Some(true))
      ),
      Set.empty,
      Set.empty
    )

    assertResult(responseFromUpdate, "Update ACL shouldn't error") {
      aclUpdateResponse
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.reader,
       testData.userReader.userEmail.value
      )
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.writer,
       testData.userReader.userEmail.value
      ),
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.canCompute,
       testData.userReader.userEmail.value
      ),
      (SamResourceTypeNames.billingProject,
       testData.workspace.namespace,
       SamBillingProjectPolicyNames.canComputeUser,
       testData.userReader.userEmail.value
      )
    )
  }

  it should "remove ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    // remove ACL
    val aclRemove = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, None))
    val aclRemoveResponse =
      Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false),
                   Duration.Inf
      )
    val responseFromRemove = WorkspaceACLUpdateResponseList(Set(
                                                              WorkspaceACLUpdate(testData.userWriter.userEmail.value,
                                                                                 WorkspaceAccessLevels.NoAccess,
                                                                                 Some(false),
                                                                                 Some(false)
                                                              )
                                                            ),
                                                            Set.empty,
                                                            Set.empty
    )

    assertResult(responseFromRemove, "Remove ACL shouldn't error") {
      aclRemoveResponse
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.canCompute,
       testData.userWriter.userEmail.value
      ),
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.writer,
       testData.userWriter.userEmail.value
      )
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq.empty
  }

  it should "remove requester pays appropriately when removing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName,
                                                   testData.userWriter.userEmail,
                                                   Set(BondServiceAccountEmail("foo@bar.com"))
      )
    )

    val aclRemove = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false),
                 Duration.Inf
    )

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)
    ) shouldBe empty
  }

  it should "keep requester pays appropriately when changing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val bondServiceAccountEmails = Set(BondServiceAccountEmail("foo@bar.com"))
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName,
                                                   testData.userWriter.userEmail,
                                                   bondServiceAccountEmails
      )
    )

    val aclUpdate = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Owner, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdate, false),
                 Duration.Inf
    )

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)
    ) should contain theSameElementsAs bondServiceAccountEmails
  }

  it should "remove requester pays appropriately when changing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName,
                                                   testData.userWriter.userEmail,
                                                   Set(BondServiceAccountEmail("foo@bar.com"))
      )
    )

    val aclUpdate = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdate, false),
                 Duration.Inf
    )

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)
    ) shouldBe empty
  }

  it should "return non-existent users during patch ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val aclUpdates = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete =
      Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false),
                   Duration.Inf
      )
    val responseFromUpdate =
      WorkspaceACLUpdateResponseList(Set.empty,
                                     Set.empty,
                                     Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
      )

    assertResult(responseFromUpdate, "Add ACL shouldn't error") {
      vComplete
    }
  }

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

  it should "pass sam read action check for a user with read access in a locked workspace" in {
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

  it should "invite a user to a workspace" in withTestDataServicesCustomSam { services =>
    val aclUpdates2 = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete2 =
      Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates2, true),
                   Duration.Inf
      )
    val responseFromUpdate2 = WorkspaceACLUpdateResponseList(
      Set.empty,
      Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, Some(true), Some(true))),
      Set.empty
    )

    assertResult(responseFromUpdate2, "Add ACL shouldn't error") {
      vComplete2
    }

    services.samDAO.invitedUsers.keySet should contain theSameElementsAs Set("obama@whitehouse.gov")
  }

  it should "retrieve catalog permission" in withTestDataServicesCustomSam { services =>
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toRawlsRequestContext(testData.userOwner))

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        testData.workspace.workspaceId,
        SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty),
        testContext
      )
    } yield ()

    Await.result(populateAcl, Duration.Inf)

    val vComplete = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
    assertResult(Set.empty) {
      vComplete.filter(wc => wc.catalog)
    }
  }

  it should "add catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))

    Await.result(for {
                   _ <- services.samDAO.registerUser(toRawlsRequestContext(user1))
                 } yield (),
                 Duration.Inf
    )

    // add catalog perm
    val catalogUpdateResponse =
      Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
                                                           Seq(WorkspaceCatalog("obama@whitehouse.gov", true))
                   ),
                   Duration.Inf
      )
    val expectedResponse =
      WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse("obama@whitehouse.gov", true)), Seq.empty)

    assertResult(expectedResponse) {
      catalogUpdateResponse
    }

    // check result
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.canCatalog,
       user1.userEmail.value
      )
    )
  }

  it should "remove catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    // remove catalog perm
    val catalogRemoveResponse = Await.result(
      services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
                                              Seq(WorkspaceCatalog(testData.userOwner.userEmail.value, false))
      ),
      Duration.Inf
    )

    val expectedResponse =
      WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse(testData.userOwner.userEmail.value, false)),
                                         Seq.empty
      )

    assertResult(expectedResponse) {
      catalogRemoveResponse
    }

    // check result
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace,
       testData.workspace.workspaceId,
       SamWorkspacePolicyNames.canCatalog,
       testData.userOwner.userEmail.value
      )
    )
  }

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
          new WorkspaceName(testData.workspaceMixedSubmissions.namespace, testData.workspaceMixedSubmissions.name)
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

    verify(services.workspaceManagerDAO, Mockito.atLeast(1)).deleteWorkspace(any[UUID], any[RawlsRequestContext])

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

  }

  it should "delete a workspace with no submissions" in withTestDataServices { services =>
    // check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    // delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    verify(services.workspaceManagerDAO, Mockito.atLeast(1)).deleteWorkspace(any[UUID], any[RawlsRequestContext])

    // check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

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

  it should "delete an Azure workspace" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription().azureContext(
        new AzureContext()
          .tenantId("fake_tenant_id")
          .subscriptionId("fake_sub_id")
          .resourceGroupId("fake_mrg_id")
      )
    )

    val workspace = Await.result(
      services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest, new ProfileModel().id(UUID.randomUUID())),
      Duration.Inf
    )
    assertResult(Option(workspace.toWorkspaceName)) {
      runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name))).map(_.toWorkspaceName)
    }

    val deletionResult = Await.result(services.workspaceService.deleteWorkspace(
                                        WorkspaceName(workspace.namespace, workspace.name)
                                      ),
                                      Duration.Inf
    )

    deletionResult.gcpContext shouldBe None
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name)))
    }
  }

  it should "not delete the rawls Azure workspace when WSM errors out for an azure workspace" in withTestDataServices {
    services =>
      val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
      val workspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        workspaceName,
        Map.empty
      )
      when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
        new WorkspaceDescription().azureContext(
          new AzureContext()
            .tenantId("fake_tenant_id")
            .subscriptionId("fake_sub_id")
            .resourceGroupId("fake_mrg_id")
        )
      )
      when(services.workspaceManagerDAO.deleteWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
        throw new ApiException("error")
      )

      val workspace =
        Await.result(services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest,
                                                                           new ProfileModel().id(UUID.randomUUID())
                     ),
                     Duration.Inf
        )
      assertResult(Option(workspace.toWorkspaceName)) {
        runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name))).map(_.toWorkspaceName)
      }

      val ex = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.deleteWorkspace(
                       WorkspaceName(workspace.namespace, workspace.name)
                     ),
                     Duration.Inf
        )
      }

      val maybeWorkspace = runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name)))
      assert(maybeWorkspace.isDefined)
  }

  it should "delete the rawls workspace when WSM returns 404" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription().azureContext(
        new AzureContext()
          .tenantId("fake_tenant_id")
          .subscriptionId("fake_sub_id")
          .resourceGroupId("fake_mrg_id")
      )
    )
    when(services.workspaceManagerDAO.deleteWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(404, "not found")
    )

    val workspace = Await.result(
      services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest, new ProfileModel().id(UUID.randomUUID())),
      Duration.Inf
    )
    assertResult(Option(workspace.toWorkspaceName)) {
      runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name))).map(_.toWorkspaceName)
    }

    Await.result(services.workspaceService.deleteWorkspace(
                   WorkspaceName(workspace.namespace, workspace.name)
                 ),
                 Duration.Inf
    )

    val maybeWorkspace = runAndWait(workspaceQuery.findByName(WorkspaceName(workspace.namespace, workspace.name)))
    assert(maybeWorkspace.isEmpty)
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
        Seq(AddListMember(AttributeName.withTagsNS, AttributeString("cancer")),
            AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe"))
        )
      ),
      Duration.Inf
    )

    Await.result(
      services.workspaceService.updateWorkspace(
        testData.wsName7,
        Seq(AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe")),
            AddListMember(AttributeName.withTagsNS, AttributeString("buffalo"))
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
      services.workspaceService.updateWorkspace(testData.wsName, Seq(RemoveAttribute(AttributeName.withTagsNS))),
      Duration.Inf
    )
    Await.result(
      services.workspaceService.updateWorkspace(testData.wsName7, Seq(RemoveAttribute(AttributeName.withTagsNS))),
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
          services.samDAO.callsToAddToPolicy should contain theSameElementsAs (Set(expectedPolicyEntry))
        } else {
          services.samDAO.callsToAddToPolicy should contain theSameElementsAs (Set.empty)
        }
    }

  val aclTestUser =
    UserInfo(RawlsUserEmail("acl-test-user"), OAuth2BearerToken(""), 0, RawlsUserSubjectId("acl-test-user-subject-id"))

  def allWorkspaceAclUpdatePermutations(emailString: String): Seq[WorkspaceACLUpdate] = for {
    accessLevel <- WorkspaceAccessLevels.all
    canShare <- Set(Some(true), Some(false), None)
    canCompute <- Set(Some(true), Some(false), None)
  } yield WorkspaceACLUpdate(emailString, accessLevel, canShare, canCompute)

  def expectedPolicies(
    aclUpdate: WorkspaceACLUpdate
  ): Either[StatusCode, Set[(SamResourceTypeName, SamResourcePolicyName)]] =
    aclUpdate match {
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.ProjectOwner, _, _) => Left(StatusCodes.BadRequest)
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Owner, _, _) =>
        Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.owner))

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, canShare, canCompute) =>
        val canSharePolicy = canShare match {
          case None | Some(false) => Set.empty
          case Some(true)         => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareWriter)
        }
        val canComputePolicy = canCompute match {
          case None | Some(true) =>
            Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.canCompute,
                SamResourceTypeNames.billingProject -> SamBillingProjectPolicyNames.canComputeUser
            )
          case Some(false) => Set.empty
        }
        Right(
          Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.writer) ++ canSharePolicy ++ canComputePolicy
        )

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, canShare, canCompute) =>
        if (canCompute.contains(true)) {
          Left(StatusCodes.BadRequest)
        } else {
          val canSharePolicy = canShare match {
            case None | Some(false) => Set.empty
            case Some(true)         => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareReader)
          }
          Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.reader) ++ canSharePolicy)
        }

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.NoAccess, _, _) => Right(Set.empty)
    }

  behavior of "aclUpdate"

  for (aclUpdate <- allWorkspaceAclUpdatePermutations(aclTestUser.userEmail.value))
    it should s"add correct policies for $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(RawlsRequestContext(aclTestUser)), Duration.Inf)
      populateWorkspacePolicies(services)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName,
                                                         Set(aclUpdate),
                                                         inviteUsersNotFound = false
                     ),
                     Duration.Inf
        )
      }

      (expectedPolicies(aclUpdate), result) match {
        case (Left(statusCode), util.Failure(exception: RawlsExceptionWithErrorReport)) =>
          assertResult(Some(statusCode), result.toString) {
            exception.errorReport.statusCode
          }

        case (Right(policies), util.Success(_)) =>
          val expectedAdds = policies.map {
            case (SamResourceTypeNames.workspace, policyName) =>
              (SamResourceTypeNames.workspace, testData.workspace.workspaceId, policyName, aclTestUser.userEmail.value)
            case (SamResourceTypeNames.billingProject, policyName) =>
              (SamResourceTypeNames.billingProject,
               testData.workspace.namespace,
               policyName,
               aclTestUser.userEmail.value
              )
            case _ => throw new Exception("make the compiler happy")
          }

          withClue(result.toString) {
            services.samDAO.callsToAddToPolicy should contain theSameElementsAs expectedAdds
            services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
          }

        case (_, r) => fail(r.toString)
      }
    }

  it should s"add correct policies for group" in withTestDataServicesCustomSam { services =>
    // setting the email to None is what a group looks like
    services.samDAO.userEmails.put(aclTestUser.userEmail.value, None)
    populateWorkspacePolicies(services)

    val aclUpdate = WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.Write)

    val result = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName,
                                                                  Set(aclUpdate),
                                                                  inviteUsersNotFound = false
                              ),
                              Duration.Inf
    )

    withClue(result.toString) {
      services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set(
        (SamResourceTypeNames.workspace,
         testData.workspace.workspaceId,
         SamWorkspacePolicyNames.writer,
         aclTestUser.userEmail.value
        ),
        (SamResourceTypeNames.workspace,
         testData.workspace.workspaceId,
         SamWorkspacePolicyNames.canCompute,
         aclTestUser.userEmail.value
        ),
        (SamResourceTypeNames.billingProject,
         testData.workspace.namespace,
         SamBillingProjectPolicyNames.canComputeUser,
         aclTestUser.userEmail.value
        )
      )
      services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
    }
  }

  it should "not clobber catalog permission" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)
    Await.result(services.samDAO.registerUser(RawlsRequestContext(aclTestUser)), Duration.Inf)

    val aclUpdate = WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.Write)
    Await.result(
      services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        testData.workspace.workspaceId,
        SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(aclUpdate.email)), Set.empty, Set(SamWorkspaceRoles.canCatalog)),
        testContext
      ),
      Duration.Inf
    )
    val result = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName,
                                                                  Set(aclUpdate),
                                                                  inviteUsersNotFound = false
                              ),
                              Duration.Inf
    )

    withClue(result.toString) {
      services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
    }
  }

  def addEmailToPolicy(services: TestApiServiceWithCustomSamDAO, policyName: SamResourcePolicyName, email: String) = {
    val policy = services.samDAO.policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId))(policyName)
    val updateMembers = policy.policy.memberEmails + WorkbenchEmail(email)
    val updatedPolicy = policy.copy(policy = policy.policy.copy(memberEmails = updateMembers))
    services.samDAO
      .policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId))
      .put(policyName, updatedPolicy)
  }

  val testPolicyNames = Set(
    SamWorkspacePolicyNames.canCompute,
    SamWorkspacePolicyNames.writer,
    SamWorkspacePolicyNames.reader,
    SamWorkspacePolicyNames.owner,
    SamWorkspacePolicyNames.projectOwner,
    SamWorkspacePolicyNames.shareReader,
    SamWorkspacePolicyNames.shareWriter
  )
  for (
    testPolicyName1 <- testPolicyNames; testPolicyName2 <- testPolicyNames
    if testPolicyName1 != testPolicyName2 && !(testPolicyName1 == SamWorkspacePolicyNames.shareReader && testPolicyName2 == SamWorkspacePolicyNames.shareWriter) && !(testPolicyName1 == SamWorkspacePolicyNames.shareWriter && testPolicyName2 == SamWorkspacePolicyNames.shareReader)
  )
    it should s"remove $testPolicyName1 and $testPolicyName2" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(RawlsRequestContext(aclTestUser)), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName1, aclTestUser.userEmail.value)
      addEmailToPolicy(services, testPolicyName2, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(
          services.workspaceService.updateACL(
            testData.workspace.toWorkspaceName,
            Set(WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.NoAccess)),
            inviteUsersNotFound = false
          ),
          Duration.Inf
        )
      }

      if (
        testPolicyName1 == SamWorkspacePolicyNames.projectOwner || testPolicyName2 == SamWorkspacePolicyNames.projectOwner
      ) {
        val error = intercept[RawlsExceptionWithErrorReport] {
          result.get
        }
        assertResult(Some(StatusCodes.BadRequest), result.toString) {
          error.errorReport.statusCode
        }
      } else {
        assert(result.isSuccess, result.toString)
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set(
          (SamResourceTypeNames.workspace,
           testData.workspace.workspaceId,
           testPolicyName1,
           aclTestUser.userEmail.value
          ),
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName2, aclTestUser.userEmail.value)
        )
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      }

    }

  for (
    testPolicyName <- Set(SamWorkspacePolicyNames.writer,
                          SamWorkspacePolicyNames.reader,
                          SamWorkspacePolicyNames.owner
    );
    aclUpdate <- Set(WorkspaceAccessLevels.Read, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Owner)
      .map(l => WorkspaceACLUpdate(aclTestUser.userEmail.value, l, canCompute = Some(false)))
  )
    it should s"change $testPolicyName to $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(RawlsRequestContext(aclTestUser)), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName,
                                                         Set(aclUpdate),
                                                         inviteUsersNotFound = false
                     ),
                     Duration.Inf
        )
      }

      assert(result.isSuccess, result.toString)

      if (aclUpdate.accessLevel.toPolicyName.contains(testPolicyName.value)) {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      } else {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set(
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName, aclTestUser.userEmail.value)
        )
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set(
          (SamResourceTypeNames.workspace,
           testData.workspace.workspaceId,
           SamResourcePolicyName(aclUpdate.accessLevel.toPolicyName.get),
           aclTestUser.userEmail.value
          )
        )
      }
    }

  "extractOperationIdsFromCromwellMetadata" should "parse workflow metadata" in {
    val jsonString =
      """{
        |  "calls": {
        |    "hello_and_goodbye.goodbye": [
        |      {
        |        "attempt": 1,
        |        "backendLogs": {
        |          "log": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-goodbye/goodbye.log"
        |        },
        |        "backendStatus": "Success",
        |        "end": "2019-04-24T13:57:48.998Z",
        |        "executionStatus": "Done",
        |        "jobId": "operations/EN2siP2kLRinu-Wt-4-bqRQgw8Sszq0dKg9wcm9kdWN0aW9uUXVldWU",
        |        "shardIndex": -1,
        |        "start": "2019-04-24T13:56:22.387Z",
        |        "stderr": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-goodbye/goodbye-stderr.log",
        |        "stdout": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-goodbye/goodbye-stdout.log"
        |      }
        |    ],
        |    "hello_and_goodbye.hello": [
        |      {
        |        "attempt": 1,
        |        "backendLogs": {
        |          "log": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-hello/hello.log"
        |        },
        |        "backendStatus": "Success",
        |        "end": "2019-04-24T13:58:21.978Z",
        |        "executionStatus": "Done",
        |        "jobId": "operations/EKCsiP2kLRiu0qj_qdLFq8wBIMPErM6tHSoPcHJvZHVjdGlvblF1ZXVl",
        |        "shardIndex": -1,
        |        "start": "2019-04-24T13:56:22.387Z",
        |        "stderr": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-hello/hello-stderr.log",
        |        "stdout": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/call-main_workflow/sub.main_workflow/1cf452d0-f18c-4945-aaf4-779402e7b2aa/call-hello_and_goodbye/sub.hello_and_goodbye/0d6768b7-73b3-41c4-b292-de743657c5db/call-hello/hello-stdout.log"
        |      }
        |    ]
        |  },
        |  "end": "2019-04-24T13:58:23.868Z",
        |  "id": "0d6768b7-73b3-41c4-b292-de743657c5db",
        |  "start": "2019-04-24T13:56:20.348Z",
        |  "status": "Succeeded",
        |  "workflowName": "sub.hello_and_goodbye",
        |  "workflowRoot": "gs://fc-2d8ada07-750f-4db8-88ab-307099d54a31/d25c4529-c247-41e0-99fb-1b8fade199d5/most_main_workflow/ccc3fdbe-3cf4-40cf-8a01-4ae77a5d3e5f/"
        |}""".stripMargin

    import spray.json._
    val metadataJson = jsonString.parseJson.asJsObject
    WorkspaceService.extractOperationIdsFromCromwellMetadata(metadataJson) should contain theSameElementsAs Seq(
      "operations/EN2siP2kLRinu-Wt-4-bqRQgw8Sszq0dKg9wcm9kdWN0aW9uUXVldWU",
      "operations/EKCsiP2kLRiu0qj_qdLFq8wBIMPErM6tHSoPcHJvZHVjdGlvblF1ZXVl"
    )
  }

  behavior of "getTerminalStatusDate"

  // test getTerminalStatusDate
  private val workflowFinishingTomorrow =
    testData.submissionMixed.workflows.head.copy(statusLastChangedDate = testDate.plusDays(1))
  private val submissionMixedDates = testData.submissionMixed.copy(
    workflows = workflowFinishingTomorrow +: testData.submissionMixed.workflows.tail
  )
  private val getTerminalStatusDateTests = Table(
    ("description", "submission", "workflowId", "expectedOutput"),
    ("submission containing one completed workflow, no workflowId input",
     testData.submissionSuccessful1,
     None,
     Option(testDate)
    ),
    ("submission containing one completed workflow, with workflowId input",
     testData.submissionSuccessful1,
     testData.submissionSuccessful1.workflows.head.workflowId,
     Option(testDate)
    ),
    ("submission containing one completed workflow, with nonexistent workflowId input",
     testData.submissionSuccessful1,
     Option("thisWorkflowIdDoesNotExist"),
     None
    ),
    ("submission containing several workflows with one finishing tomorrow, no workflowId input",
     submissionMixedDates,
     None,
     Option(testDate.plusDays(1))
    ),
    ("submission containing several workflows, with workflowId input for workflow finishing tomorrow",
     submissionMixedDates,
     submissionMixedDates.workflows.head.workflowId,
     Option(testDate.plusDays(1))
    ),
    ("submission containing several workflows, with workflowId input for workflow finishing today",
     submissionMixedDates,
     submissionMixedDates.workflows(2).workflowId,
     Option(testDate)
    ),
    ("submission containing several workflows, with workflowId input for workflow not finished",
     submissionMixedDates,
     submissionMixedDates.workflows.last.workflowId,
     None
    ),
    ("aborted submission, no workflowId input", testData.submissionAborted1, None, Option(testDate)),
    ("aborted submission, with workflowId input",
     testData.submissionAborted1,
     testData.submissionAborted1.workflows.head.workflowId,
     Option(testDate)
    ),
    ("in progress submission, no workflowId input", testData.submissionSubmitted, None, None),
    ("in progress submission, with workflowId input",
     testData.submissionSubmitted,
     testData.submissionSubmitted.workflows.head.workflowId,
     None
    )
  )
  forAll(getTerminalStatusDateTests) { (description, submission, workflowId, expectedOutput) =>
    it should s"run getTerminalStatusDate test for $description" in {
      assertResult(WorkspaceService.getTerminalStatusDate(submission, workflowId))(expectedOutput)
    }
  }

  behavior of "RequesterPays"

  it should "return Unit when adding linked service accounts to workspace" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete =
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
      withWorkspaceContext(testData.workspace) { ctx =>
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
    withWorkspaceContext(testData.workspace) { ctx =>
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
    withWorkspaceContext(testData.workspace) { ctx =>
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
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete =
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
      withWorkspaceContext(testData.workspace) { ctx =>
        val rqComplete = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(
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
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete =
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
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete =
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

  it should "create Sam resource for google project" in withTestDataServices { services =>
    val newWorkspaceName = "new-workspace"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    // Verify that samDAO.createResourceFull was called
    verify(services.samDAO).createResourceFull(
      ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
      ArgumentMatchers.eq(workspace.googleProjectId.value),
      any[Map[SamResourcePolicyName, SamPolicy]],
      any[Set[String]],
      any[RawlsRequestContext],
      any[Option[SamFullyQualifiedResourceId]]
    )
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
        any[OpenCensusSpan]
      )
  }

  it should "fail to create a database object when GoogleServicesDAO throws an exception when updating billing account" in withTestDataServices {
    services =>
      doReturn(Future.failed(new Exception("failed")), null)
        .when(services.gcsDAO)
        .setBillingAccountName(
          ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
          ArgumentMatchers.eq(RawlsBillingAccountName("fakeBillingAcct")),
          any[OpenCensusSpan]
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

    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    verify(services.resourceBufferService).getGoogleProjectFromBuffer(any[ProjectPoolType], any[String])
  }

  it should "Update a Google Project name after claiming a project from Resource Buffering Service" in withTestDataServices {
    services =>
      val newWorkspaceNamespace = "short_-NS1"
      val newWorkspaceName =
        "plus Long_ name to get past 30 chars since the google-project name is truncated at 30 chars and formatted as namespace--name"
      val billingProject = RawlsBillingProject(RawlsBillingProjectName(newWorkspaceNamespace),
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

      val expectedProjectName = "short--NS1--plus Long- name to"
      val actualProjectName = capturedProject.getName
      actualProjectName shouldBe expectedProjectName
  }

  it should "Apply labels to a Google Project after claiming a project from Resource Buffering Service" in withTestDataServices {
    services =>
      val newWorkspaceNamespace = "Long_Namespace---30-char-limit"
      val newWorkspaceName = "Plus Long_ name to get past 63 chars since the labels are truncated at 63 chars"
      val billingProject = RawlsBillingProject(RawlsBillingProjectName(newWorkspaceNamespace),
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

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "creating a Workspace in a Service Perimeter" should "attempt to overwrite the correct Service Perimeter" in withTestDataServices {
    services =>
      // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
      val servicePerimeterName: ServicePerimeterName =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters.keys.head
      val staticProjectNumbersInPerimeter: Set[String] =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters(servicePerimeterName).map(_.value).toSet

      val billingProject1 = testData.testProject1
      val billingProject2 = testData.testProject2
      val billingProjects = Seq(billingProject1, billingProject2)
      val workspacesPerProject = 2

      // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
      // the Billing Projects and therefore in the Perimeter
      val workspacesInPerimeter: Seq[Workspace] = billingProjects.flatMap { bp =>
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

  "cloneWorkspace" should "create a V2 Workspace" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace =
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
                                                                        baseWorkspace.toWorkspaceName,
                                                                        workspaceRequest
                   ),
                   Duration.Inf
      )

    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.googleProjectId.value should not be empty
    workspace.googleProjectNumber should not be empty
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
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
    workspace.googleProjectId.value should not be empty
    workspace.googleProjectNumber should not be empty
  }

  it should "fail with 400 if specified Namespace/Billing Project does not exist" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest("nonexistent_namespace", "kermits_pond", Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
          services.mcWorkspaceService.cloneMultiCloudWorkspace(
            services.workspaceService,
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
        any[OpenCensusSpan]
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
          any[OpenCensusSpan]
        )

      intercept[Exception] {
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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

    val workspace =
      Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
                                                                          baseWorkspace.toWorkspaceName,
                                                                          workspaceRequest
                     ),
                     Duration.Inf
        )

      workspace.bucketName should startWith(s"${services.workspaceServiceConfig.workspaceBucketNamePrefix}-secure")
  }

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "cloning a Workspace into a Service Perimeter" should "attempt to overwrite the correct Service Perimeter" in withTestDataServices {
    services =>
      // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
      val servicePerimeterName: ServicePerimeterName =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters.keys.head
      val staticProjectNumbersInPerimeter: Set[String] =
        services.servicePerimeterServiceConfig.staticProjectsInPerimeters(servicePerimeterName).map(_.value).toSet

      val billingProject1 = testData.testProject1
      val billingProject2 = testData.testProject2
      val billingProjects = Seq(billingProject1, billingProject2)
      val workspacesPerProject = 2

      // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
      // the Billing Projects and therefore in the Perimeter
      val workspacesInPerimeter: Seq[Workspace] = billingProjects.flatMap { bp =>
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
        Await.result(services.mcWorkspaceService.cloneMultiCloudWorkspace(services.workspaceService,
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

  "getSpendReportTableName" should "return the correct fully formatted BigQuery table name if the spend report config is set" in withTestDataServices {
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

      val result = Await.result(services.workspaceService.getSpendReportTableName(billingProjectName), Duration.Inf)

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

    val result = Await.result(services.workspaceService.getSpendReportTableName(billingProjectName), Duration.Inf)

    result shouldBe None
  }

  it should "throw a RawlsExceptionWithErrorReport if the billing project does not exist" in withTestDataServices {
    services =>
      val billingProjectName = RawlsBillingProjectName("test-project")

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.getSpendReportTableName(billingProjectName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.NotFound
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
    response.bucketOptions shouldBe Some(WorkspaceBucketOptions(false))
    response.azureContext shouldEqual None
    response.workspace.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    response.workspace.state shouldBe WorkspaceState.Ready
  }

  private def createAzureWorkspace(services: TestApiService,
                                   managedAppCoordinates: AzureManagedAppCoordinates,
                                   policies: List[WsmPolicyInput] = List()
  ): Workspace = {
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"

    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription()
        .azureContext(
          new AzureContext()
            .tenantId(managedAppCoordinates.tenantId.toString)
            .subscriptionId(managedAppCoordinates.subscriptionId.toString)
            .resourceGroupId(managedAppCoordinates.managedResourceGroupId)
        )
        .policies(policies.asJava)
    )

    Await.result(
      services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest, new ProfileModel().id(UUID.randomUUID())),
      Duration.Inf
    )
  }

  it should "get the details of an Azure workspace" in withTestDataServices { services =>
    val managedAppCoordinates = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg_id")
    val workspace = createAzureWorkspace(services, managedAppCoordinates)
    val readWorkspace = Await.result(services.workspaceService.getWorkspace(
                                       WorkspaceName(workspace.namespace, workspace.name),
                                       WorkspaceFieldSpecs()
                                     ),
                                     Duration.Inf
    )

    val response = readWorkspace.convertTo[WorkspaceResponse]

    response.azureContext.get.tenantId.toString shouldEqual managedAppCoordinates.tenantId.toString
    response.azureContext.get.subscriptionId.toString shouldEqual managedAppCoordinates.subscriptionId.toString
    response.azureContext.get.managedResourceGroupId shouldEqual managedAppCoordinates.managedResourceGroupId
    response.workspace.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
    response.workspace.state shouldBe WorkspaceState.Ready
  }

  it should "return the policies of an Azure workspace" in withTestDataServices { services =>
    val managedAppCoordinates = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg_id")
    val wsmPolicyInput = new WsmPolicyInput()
      .name("test_name")
      .namespace("test_namespace")
      .additionalData(
        List(
          new WsmPolicyPair().value("pair1Val").key("pair1Key"),
          new WsmPolicyPair().value("pair2Val").key("pair2Key")
        ).asJava
      )
    val workspace = createAzureWorkspace(services, managedAppCoordinates, List(wsmPolicyInput))
    val readWorkspace = Await.result(services.workspaceService.getWorkspace(
                                       WorkspaceName(workspace.namespace, workspace.name),
                                       WorkspaceFieldSpecs()
                                     ),
                                     Duration.Inf
    )

    val response = readWorkspace.convertTo[WorkspaceResponse]
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

  it should "return correct canCompute permission for Azure workspaces" in withTestDataServices { services =>
    val managedAppCoordinates = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg_id")
    val workspace = createAzureWorkspace(services, managedAppCoordinates)

    // Create test user.
    val testUser = RawlsUser(RawlsUserSubjectId("testuser"), RawlsUserEmail("test@user.com"))
    val ctx = toRawlsRequestContext(testUser)

    // Mock user being a reader only
    val mockSamDAO = services.samDAO
    val testUserWS = services.workspaceServiceConstructor(ctx)
    when(
      mockSamDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx)
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx)
    ).thenReturn(Future.successful(false))
    when(
      mockSamDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.own, ctx)
    ).thenReturn(Future.successful(false))
    when(
      mockSamDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
    )
      .thenReturn(Future.successful(Set(SamWorkspaceRoles.reader)))

    val readerWorkspace =
      Await.result(testUserWS.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs()), Duration.Inf)
    val readerResponse = readerWorkspace.convertTo[WorkspaceResponse]
    readerResponse.canCompute.get shouldEqual false
    readerResponse.accessLevel.get shouldEqual WorkspaceAccessLevels.Read

    // Now change mocks to be a writer.
    when(
      mockSamDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx)
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
    )
      .thenReturn(Future.successful(Set(SamWorkspaceRoles.writer, SamWorkspaceRoles.reader)))

    val writerWorkspace =
      Await.result(testUserWS.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs()), Duration.Inf)
    val writerResponse = writerWorkspace.convertTo[WorkspaceResponse]
    writerResponse.canCompute.get shouldEqual true
    writerResponse.accessLevel.get shouldEqual WorkspaceAccessLevels.Write

    // Now change mocks to be an owner.
    when(
      mockSamDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.own, ctx)
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
    )
      .thenReturn(Future.successful(Set(SamWorkspaceRoles.owner)))

    val ownerWorkspace =
      Await.result(testUserWS.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs()), Duration.Inf)
    val ownerResponse = ownerWorkspace.convertTo[WorkspaceResponse]
    ownerResponse.canCompute.get shouldEqual true
    ownerResponse.accessLevel.get shouldEqual WorkspaceAccessLevels.Owner
  }

  it should "return an error if an MC workspace is not present in workspace manager" in withTestDataServices {
    services =>
      val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
      val workspaceRequest = WorkspaceRequest(
        testData.testProject1Name.value,
        workspaceName,
        Map.empty
      )
      // ApiException is a checked exception so we need to use thenAnswer rather than thenThrow
      when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
        throw new ApiException(StatusCodes.NotFound.intValue, "not found")
      )
      val workspace = Await.result(
        services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest,
                                                              new ProfileModel().id(UUID.randomUUID())
        ),
        Duration.Inf
      )

      val err = intercept[AggregateWorkspaceNotFoundException] {
        Await.result(services.workspaceService.getWorkspace(
                       WorkspaceName(workspace.namespace, workspace.name),
                       WorkspaceFieldSpecs()
                     ),
                     Duration.Inf
        )
      }

      withClue("MC workspace not present in WSM should result in 404") {
        err.errorReport.statusCode shouldBe Some(StatusCodes.NotFound)
      }
  }

  it should "return an error if an MC workspace does not have an Azure context" in withTestDataServices { services =>
    val workspaceName = s"rawls-test-workspace-${UUID.randomUUID().toString}"
    val workspaceRequest = WorkspaceRequest(
      testData.testProject1Name.value,
      workspaceName,
      Map.empty
    )
    when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription() // no azureContext, should be an error
    )
    val workspace = Await.result(
      services.mcWorkspaceService.createMultiCloudWorkspace(workspaceRequest, new ProfileModel().id(UUID.randomUUID())),
      Duration.Inf
    )

    val err = intercept[InvalidCloudContextException] {
      Await.result(services.workspaceService.getWorkspace(
                     WorkspaceName(workspace.namespace, workspace.name),
                     WorkspaceFieldSpecs()
                   ),
                   Duration.Inf
      )
    }

    withClue("MC workspace with no azure context should result in not implemented") {
      err.errorReport.statusCode shouldBe Some(StatusCodes.NotImplemented)
    }
  }

  behavior of "listWorkspaces"

  it should "list the correct cloud platform and state for Azure and Google workspaces" in withTestDataServices {
    services =>
      val service = services.workspaceService
      val workspaceId1 = UUID.randomUUID().toString
      val workspaceId2 = UUID.randomUUID().toString

      // set up test data
      val azureWorkspace =
        Workspace.buildReadyMcWorkspace("test_namespace1",
                                        "name",
                                        workspaceId1,
                                        new DateTime(),
                                        new DateTime(),
                                        "testUser1",
                                        Map.empty
        )
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
      val azureWorkspaceDetails =
        WorkspaceDetails.fromWorkspaceAndOptions(azureWorkspace, Some(Set()), true, Some(WorkspaceCloudPlatform.Azure))
      val googleWorkspaceDetails =
        WorkspaceDetails.fromWorkspaceAndOptions(googleWorkspace, Some(Set()), true, Some(WorkspaceCloudPlatform.Gcp))
      val expected = List(
        (azureWorkspaceDetails.workspaceId, azureWorkspaceDetails.cloudPlatform, azureWorkspaceDetails.state),
        (googleWorkspaceDetails.workspaceId, googleWorkspaceDetails.cloudPlatform, googleWorkspaceDetails.state)
      )

      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(azureWorkspace)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleWorkspace)
        } yield ()
      }

      // mock external calls
      when(service.workspaceManagerDAO.getWorkspace(azureWorkspace.workspaceIdAsUUID, services.ctx1))
        .thenReturn(
          new WorkspaceDescription().azureContext(
            new AzureContext()
              .tenantId(UUID.randomUUID.toString)
              .subscriptionId(UUID.randomUUID.toString)
              .resourceGroupId(UUID.randomUUID.toString)
          )
        )
      when(service.workspaceManagerDAO.getWorkspace(googleWorkspace.workspaceIdAsUUID, services.ctx1)).thenReturn(
        new WorkspaceDescription().gcpContext(new GcpContext())
      )
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

  it should "not return a MC workspace that does not have a cloud context" in withTestDataServices { services =>
    val service = services.workspaceService
    val workspaceId1 = UUID.randomUUID().toString
    val workspaceId2 = UUID.randomUUID().toString

    // set up test data
    val azureWorkspace =
      Workspace.buildReadyMcWorkspace("test_namespace1",
                                      "name",
                                      workspaceId1,
                                      new DateTime(),
                                      new DateTime(),
                                      "testUser1",
                                      Map.empty
      )
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

    runAndWait {
      for {
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(azureWorkspace)
        _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleWorkspace)
      } yield ()
    }

    when(service.workspaceManagerDAO.getWorkspace(azureWorkspace.workspaceIdAsUUID, services.ctx1))
      .thenReturn(new WorkspaceDescription()) // no azureContext, should not be returned
    when(service.workspaceManagerDAO.getWorkspace(googleWorkspace.workspaceIdAsUUID, services.ctx1)).thenReturn(
      new WorkspaceDescription().gcpContext(new GcpContext())
    )
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

    val result =
      Await
        .result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
        .convertTo[Seq[WorkspaceListResponse]]
    val expected = List((googleWorkspace.workspaceId, Some(WorkspaceCloudPlatform.Gcp)))
    result.map(ws => (ws.workspace.workspaceId, ws.workspace.cloudPlatform)) should contain theSameElementsAs expected
  }

  it should "log a warning and filter out the workspace if WSM's getWorkspace throws an ApiException" in withTestDataServices {
    services =>
      val service = services.workspaceService
      val workspaceId1 = UUID.randomUUID().toString
      val workspaceId2 = UUID.randomUUID().toString

      // set up test data
      val azureWorkspace =
        Workspace.buildReadyMcWorkspace("test_namespace1",
                                        "name",
                                        workspaceId1,
                                        new DateTime(),
                                        new DateTime(),
                                        "testUser1",
                                        Map.empty
        )
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
      val expected = List((googleWorkspaceDetails.workspaceId, googleWorkspaceDetails.cloudPlatform))

      runAndWait {
        for {
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(azureWorkspace)
          _ <- slickDataSource.dataAccess.workspaceQuery.createOrUpdate(googleWorkspace)
        } yield ()
      }

      when(service.workspaceManagerDAO.getWorkspace(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID), any()))
        .thenAnswer(_ => throw new ApiException(StatusCodes.NotFound.intValue, "not found"))
      when(service.workspaceManagerDAO.getWorkspace(ArgumentMatchers.eq(googleWorkspace.workspaceIdAsUUID), any()))
        .thenReturn(
          new WorkspaceDescription().gcpContext(new GcpContext())
        )
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

      val result =
        Await
          .result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)
          .convertTo[Seq[WorkspaceListResponse]]

      result.map(ws => (ws.workspace.workspaceId, ws.workspace.cloudPlatform)) should contain theSameElementsAs expected
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

  "getSubmissionMethodConfiguration" should "return the method configuration that was used to launch the submission" in withTestDataServices {
    services =>
      val workspaceName = testData.workspaceSuccessfulSubmission.toWorkspaceName
      val originalMethodConfig = testData.agoraMethodConfig

      // Overwrite the method configuration that was used for the submission. This forces it to generate a new version and soft-delete the old one
      Await.result(
        services.workspaceService.overwriteMethodConfiguration(
          workspaceName,
          originalMethodConfig.namespace,
          originalMethodConfig.name,
          originalMethodConfig.copy(inputs = Map("i1" -> AttributeString("input_updated")))
        ),
        Duration.Inf
      )

      val firstSubmission =
        Await.result(services.workspaceService.listSubmissions(workspaceName, testContext), Duration.Inf).head

      val result = Await.result(
        services.workspaceService.getSubmissionMethodConfiguration(workspaceName, firstSubmission.submissionId),
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
}
