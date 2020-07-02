package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.PoisonPill
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{CustomizableMockSamDAO, MockBondApiDAO, MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.{DataRepoEntityProviderConfig, DeploymentManagerConfig, MethodRepoConfig}
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito.verify

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.Try


//noinspection NameBooleanParameters,TypeAnnotation,EmptyParenMethodAccessedAsParameterless,ScalaUnnecessaryParentheses,RedundantNewCaseClass,ScalaUnusedSymbol
class WorkspaceServiceSpec extends FlatSpec with ScalatestRouteTest with Matchers with TestDriverComponent with RawlsTestUtils with Eventually with MockitoTestUtils with RawlsStatsDTestUtils with BeforeAndAfterAll {
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

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(userInfo1)
    lazy val userService: UserService = userServiceConstructor(userInfo1)


    def actorRefFactory = system
    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
    val samDAO = new MockSamDAO(dataSource)
    val gpsDAO = new MockGooglePubSubDAO
    val workspaceManagerDAO = mock[MockWorkspaceManagerDAO]
    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO()

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    val testConf = ConfigFactory.load()

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      new UncoordinatedDataSourceAccess(slickDataSource),
      samDAO,
      gcsDAO,
      gcsDAO.getBucketServiceAccountCredential,
      SubmissionMonitorConfig(1 second, true),
      workbenchMetricBaseName = "test"
    ).withDispatcher("submission-monitor-dispatcher"))

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      notificationDAO,
      samDAO,
      Seq("bigquery.jobUser"),
      "requesterPaysRole",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate"))
    )_

    val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService("test", "test", bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-"
    )

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService = new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()
    val entityManager = EntityManager.defaultEntityManager(dataSource, workspaceManagerDAO, dataRepoDAO, samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10))

    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(
        MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
        MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
        workbenchMetricBaseName = workbenchMetricBaseName),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
      execServiceBatchSize,
      workspaceManagerDAO,
      dataRepoDAO,
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
      entityManager
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  class TestApiServiceWithCustomSamDAO(dataSource: SlickDataSource, override val user: RawlsUser)(override implicit val executionContext: ExecutionContext) extends TestApiService(dataSource, user) {
    override val samDAO: CustomizableMockSamDAO = new CustomizableMockSamDAO(dataSource)
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  def withTestDataServicesCustomSamAndUser[T](user: RawlsUser)(testCode: TestApiServiceWithCustomSamDAO => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServicesCustomSam(dataSource, user)(testCode)
    }
  }

  def withTestDataServicesCustomSam[T](testCode: TestApiServiceWithCustomSamDAO => T): T = {
    withTestDataServicesCustomSamAndUser(testData.userOwner)(testCode)
  }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  private def withServicesCustomSam[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiServiceWithCustomSamDAO) => T) = {
    val apiService = new TestApiServiceWithCustomSamDAO(dataSource, user)

    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  it should "retrieve ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val vComplete = Await.result(services.workspaceService.getACL(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACL)]]
    val (vStatus, vData) = vComplete.response

    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(WorkspaceACL(Map(
      testData.userOwner.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
      testData.userWriter.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Write, false, false, true),
      testData.userReader.userEmail.value -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)))) {
      vData
    }
  }

  private def toUserInfo(user: RawlsUser) = UserInfo(user.userEmail, OAuth2BearerToken(""), 0, user.userSubjectId)
  private def populateWorkspacePolicies(services: TestApiService, workspace: Workspace = testData.workspace) = {
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toUserInfo(testData.userOwner))
      _ <- services.samDAO.registerUser(toUserInfo(testData.userWriter))
      _ <- services.samDAO.registerUser(toUserInfo(testData.userReader))

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.owner,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.own, SamWorkspaceActions.write, SamWorkspaceActions.read), Set(SamWorkspaceRoles.owner)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.writer,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)), Set(SamWorkspaceActions.write, SamWorkspaceActions.read), Set(SamWorkspaceRoles.writer)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.reader,
        SamPolicy(Set(WorkbenchEmail(testData.userReader.userEmail.value)), Set(SamWorkspaceActions.read), Set(SamWorkspaceRoles.reader)), userInfo)

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.shareReader,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.shareWriter,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.canCompute,
        SamPolicy(Set(WorkbenchEmail(testData.userWriter.userEmail.value)), Set.empty, Set.empty), userInfo)
      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.projectOwner,
        SamPolicy(Set.empty, Set.empty, Set.empty), userInfo)
    } yield ()

    Await.result(populateAcl, Duration.Inf)
  }

  it should "add ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    val user2 = RawlsUser(RawlsUserSubjectId("obamaiscool2"), RawlsUserEmail("obama2@whitehouse.gov"))

    Await.result(for {
      _ <- services.samDAO.registerUser(toUserInfo(user1))
      _ <- services.samDAO.registerUser(toUserInfo(user2))
    } yield (), Duration.Inf)

    //add ACL
    val aclAdd = Set(WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, None), WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Option(true)))
    val aclAddResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclAdd, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromAdd = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(user1.userEmail.value, WorkspaceAccessLevels.Owner, Some(true), Some(true)), WorkspaceACLUpdate(user2.userEmail.value, WorkspaceAccessLevels.Read, Some(true), Some(false))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromAdd), aclAddResponse.response.toString()) {
      aclAddResponse.response
    }

    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.owner, user1.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.shareReader, user2.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.reader, user2.userEmail.value)
    )
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq.empty
  }

  it should "update ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //update ACL
    val aclUpdates = Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None))
    val aclUpdateResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, Some(false), Some(true))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate), "Update ACL shouldn't error") {
      aclUpdateResponse.response
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.reader, testData.userReader.userEmail.value)
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, testData.userReader.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, testData.userReader.userEmail.value),
      (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, testData.userReader.userEmail.value)
    )
  }

  it should "remove ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //remove ACL
    val aclRemove = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, None))
    val aclRemoveResponse = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, List[WorkspaceACLUpdateResponseList])]]
    val responseFromRemove = WorkspaceACLUpdateResponseList(Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, Some(false), Some(false))), Set.empty, Set.empty)

    assertResult((StatusCodes.OK, responseFromRemove), "Remove ACL shouldn't error") {
      aclRemoveResponse.response
    }

    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, testData.userWriter.userEmail.value),
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, testData.userWriter.userEmail.value)
    )
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq.empty
  }

  it should "remove requester pays appropriately when removing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    runAndWait(workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail, Set(BondServiceAccountEmail("foo@bar.com"))))

    val aclRemove = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.NoAccess, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclRemove, false), Duration.Inf)

    runAndWait(workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)) shouldBe empty
  }

  it should "keep requester pays appropriately when changing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    runAndWait(workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail, Set(BondServiceAccountEmail("foo@bar.com"))))

    val aclUpdate = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Owner, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    runAndWait(workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)) should contain theSameElementsAs(Set("foo@bar.com"))
  }

  it should "remove requester pays appropriately when changing ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    runAndWait(workspaceRequesterPaysQuery.insertAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail, Set(BondServiceAccountEmail("foo@bar.com"))))

    val aclUpdate = Set(WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None))
    Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdate, false), Duration.Inf)

    runAndWait(workspaceRequesterPaysQuery.listAllForUser(testData.workspace.toWorkspaceName, testData.userWriter.userEmail)) shouldBe empty
  }

  it should "return non-existent users during patch ACLs" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val aclUpdates = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates, false), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate = WorkspaceACLUpdateResponseList(Set.empty, Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None)))

    assertResult((StatusCodes.OK, responseFromUpdate), "Add ACL shouldn't error") {
      vComplete.response
    }
  }

  it should "pass sam read action check for a user with read access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.read), Duration.Inf).asInstanceOf[RequestComplete[StatusCode]]
    assertResult(StatusCodes.NoContent) {
      rqComplete.response
    }
  }

  it should "pass sam read action check for a user with read access in a locked workspace" in {
    withTestDataServicesCustomSam { services =>
      populateWorkspacePolicies(services, testData.workspaceNoSubmissions) //can't lock a workspace with running submissions, which the default workspace has
      Await.result(services.workspaceService.lockWorkspace(testData.workspaceNoSubmissions.toWorkspaceName), Duration.Inf)

      //generate a new workspace service with a reader user info so we can ask if a reader can access it
      val readerWorkspaceService = services.workspaceServiceConstructor(UserInfo(testData.userReader.userEmail, OAuth2BearerToken("token"), 0, testData.userReader.userSubjectId))
      val rqComplete = Await.result(readerWorkspaceService.checkSamActionWithLock(testData.workspaceNoSubmissions.toWorkspaceName, SamWorkspaceActions.read), Duration.Inf).asInstanceOf[RequestComplete[StatusCode]]
      assertResult(StatusCodes.NoContent) {
        rqComplete.response
      }
    }
  }

  it should "fail sam write action check for a user with read access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.write), Duration.Inf).asInstanceOf[RequestComplete[StatusCode]]
    assertResult(StatusCodes.Forbidden) {
      rqComplete.response
    }
  }

  it should "pass sam write action check for a user with write access in an unlocked workspace" in withTestDataServicesCustomSamAndUser(testData.userWriter) { services =>
    populateWorkspacePolicies(services)
    val rqComplete = Await.result(services.workspaceService.checkSamActionWithLock(testData.workspace.toWorkspaceName, SamWorkspaceActions.write), Duration.Inf).asInstanceOf[RequestComplete[StatusCode]]
    assertResult(StatusCodes.NoContent) {
      rqComplete.response
    }
  }

  //this is the important test!
  it should "fail sam write action check for a user with write access in a locked workspace" in withTestDataServicesCustomSam { services =>
    //first lock the workspace as the owner
    populateWorkspacePolicies(services, testData.workspaceNoSubmissions) //can't lock a workspace with running submissions, which default workspace has
    Await.result(services.workspaceService.lockWorkspace(testData.workspaceNoSubmissions.toWorkspaceName), Duration.Inf)

    //now as a writer, ask if we can write it. but it's locked!
    val readerWorkspaceService = services.workspaceServiceConstructor(UserInfo(testData.userWriter.userEmail, OAuth2BearerToken("token"), 0, testData.userWriter.userSubjectId))
    val rqComplete = Await.result(readerWorkspaceService.checkSamActionWithLock(testData.workspaceNoSubmissions.toWorkspaceName, SamWorkspaceActions.write), Duration.Inf).asInstanceOf[RequestComplete[StatusCode]]
    assertResult(StatusCodes.Forbidden) {
      rqComplete.response
    }
  }

  it should "invite a user to a workspace" in withTestDataServicesCustomSam { services =>
    val aclUpdates2 = Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, None))
    val vComplete2 = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, aclUpdates2, true), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceACLUpdateResponseList)]]
    val responseFromUpdate2 = WorkspaceACLUpdateResponseList(Set.empty, Set(WorkspaceACLUpdate("obama@whitehouse.gov", WorkspaceAccessLevels.Owner, Some(true), Some(true))), Set.empty)

    assertResult((StatusCodes.OK, responseFromUpdate2), "Add ACL shouldn't error") {
      vComplete2.response
    }

    services.samDAO.invitedUsers.keySet should contain theSameElementsAs Set("obama@whitehouse.gov")
  }

  it should "retrieve catalog permission" in withTestDataServicesCustomSam { services =>
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toUserInfo(testData.userOwner))

      _ <- services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog,
        SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set(SamWorkspaceActions.catalog), Set.empty), userInfo)
    } yield ()

    Await.result(populateAcl, Duration.Inf)

    val vComplete = Await.result(services.workspaceService.getCatalog(testData.workspace.toWorkspaceName), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Set[WorkspaceCatalog])]]
    val (vStatus, vData) = vComplete.response
    assertResult((StatusCodes.OK, Set.empty)) {
      (vStatus, vData.filter(wc => wc.catalog))
    }
  }

  it should "add catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    val user1 = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))

    Await.result(for {
      _ <- services.samDAO.registerUser(toUserInfo(user1))
    } yield (), Duration.Inf)

    //add catalog perm
    val catalogUpdateResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog("obama@whitehouse.gov", true))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]
    val expectedResponse = WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse("obama@whitehouse.gov", true)), Seq.empty)

    assertResult((StatusCodes.OK, expectedResponse)) {
      catalogUpdateResponse.response
    }

    //check result
    services.samDAO.callsToAddToPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog, user1.userEmail.value)
    )
  }

  it should "remove catalog permissions" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)

    //remove catalog perm
    val catalogRemoveResponse = Await.result(services.workspaceService.updateCatalog(testData.workspace.toWorkspaceName,
      Seq(WorkspaceCatalog(testData.userOwner.userEmail.value, false))), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, WorkspaceCatalogUpdateResponseList)]]

    val expectedResponse = WorkspaceCatalogUpdateResponseList(Seq(WorkspaceCatalogResponse(testData.userOwner.userEmail.value, false)), Seq.empty)

    assertResult((StatusCodes.OK, expectedResponse)) {
      catalogRemoveResponse.response
    }

    //check result
    services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Seq(
      (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog, testData.userOwner.userEmail.value)
    )
  }

  it should "lock a workspace with terminated submissions" in withTestDataServices { services =>
    //check workspace is not locked
    assert(!testData.workspaceTerminatedSubmissions.isLocked)

    val rqComplete = Await.result(services.workspaceService.lockWorkspace(testData.workspaceTerminatedSubmissions.toWorkspaceName), Duration.Inf)
        .asInstanceOf[RequestComplete[StatusCode]]

    assertResult(StatusCodes.NoContent) {
      rqComplete.response
    }

    //check workspace is locked
    assert {
      runAndWait(workspaceQuery.findByName(testData.workspaceTerminatedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "fail to lock a workspace with active submissions" in withTestDataServices { services =>
    //check workspace is not locked
    assert(!testData.workspaceMixedSubmissions.isLocked)

   val except: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
     Await.result(services.workspaceService.lockWorkspace(new WorkspaceName(testData.workspaceMixedSubmissions.namespace, testData.workspaceMixedSubmissions.name)), Duration.Inf)
   }

    assertResult(StatusCodes.Conflict) {
      except.errorReport.statusCode.get
    }

    assert {
      !runAndWait(workspaceQuery.findByName(testData.workspaceMixedSubmissions.toWorkspaceName)).head.isLocked
    }
  }

  it should "delete a workspace with no submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceNoSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName3), Duration.Inf)

    verify(services.workspaceManagerDAO, Mockito.atLeast(1)).deleteWorkspace(any[UUID], any[OAuth2BearerToken], any[OAuth2BearerToken])

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName3))
    }


  }

  it should "delete a workspace with succeeded submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSuccessfulSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig2",Some("Sample"),AgoraMethod("myNamespace","method-a",1),"dsde"),
      MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSuccessfulSubmission))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSuccessful1)) {
      runAndWait(submissionQuery.list(testData.workspaceSuccessfulSubmission))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName4), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName4))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSuccessfulSubmission))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceSuccessfulSubmission))
    }

    //Check if entities on workspace have been deleted
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with failed submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceFailedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceFailedSubmission))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionFailed)) {
      runAndWait(submissionQuery.list(testData.workspaceFailedSubmission))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName5), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName5))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceFailedSubmission))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceFailedSubmission))
    }


    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceFailedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with submitted submission" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceSubmittedSubmission)) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSubmittedSubmission))
    }

    //Check if submissions on workspace exist
    assertResult(List(testData.submissionSubmitted)) {
      runAndWait(submissionQuery.list(testData.workspaceSubmittedSubmission))
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName6), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName6))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceSubmittedSubmission))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceSubmittedSubmission))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceSubmittedSubmission.workspaceId)).length.result)
    }
  }

  it should "delete a workspace with mixed submissions" in withTestDataServices { services =>
    //check that the workspace to be deleted exists
    assertWorkspaceResult(Option(testData.workspaceMixedSubmissions)) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //Check method configs to be deleted exist
    assertResult(Vector(MethodConfigurationShort("testConfig1",Some("Sample"),AgoraMethod("ns-config","meth1",1),"ns"))) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceMixedSubmissions))
    }

    //Check if submissions on workspace exist
    assertResult(2) {
      runAndWait(submissionQuery.list(testData.workspaceMixedSubmissions)).length
    }

    //Check if entities on workspace exist
    assertResult(20) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

    //delete the workspace
    Await.result(services.workspaceService.deleteWorkspace(testData.wsName7), Duration.Inf)

    //check that the workspace has been deleted
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.wsName7))
    }

    //check if method configs have been deleted
    assertResult(Vector()) {
      runAndWait(methodConfigurationQuery.listActive(testData.workspaceMixedSubmissions))
    }

    //Check if submissions on workspace have been deleted
    assertResult(Vector()) {
      runAndWait(submissionQuery.list(testData.workspaceMixedSubmissions))
    }

    //Check if entities on workspace exist
    assertResult(0) {
      runAndWait(entityQuery.findActiveEntityByWorkspace(UUID.fromString(testData.workspaceMixedSubmissions.workspaceId)).length.result)
    }

  }

  it should "return the correct tags from autocomplete" in withTestDataServices { services =>

    // when no tags, return empty set
    val res1 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector.empty[WorkspaceTag]) {
      res1.response._2
    }

    // add some tags
    Await.result(services.workspaceService.updateWorkspace(testData.wsName,
      Seq(AddListMember(AttributeName.withTagsNS, AttributeString("cancer")),
        AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe")))), Duration.Inf)

    Await.result(services.workspaceService.updateWorkspace(testData.wsName7,
      Seq(
        AddListMember(AttributeName.withTagsNS, AttributeString("cantaloupe")),
        AddListMember(AttributeName.withTagsNS, AttributeString("buffalo")))), Duration.Inf)

    // searching for tag that doesn't exist should return empty set
    val res2 = Await.result(services.workspaceService.getTags(Some("notag")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[String])]]
    assertResult(Vector.empty[String]) {
      res2.response._2
    }

    // searching for tag that does exist should return the tag (query string case doesn't matter)
    val res3 = Await.result(services.workspaceService.getTags(Some("bUf")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("buffalo", 1))) {
      res3.response._2
    }

    val res4 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cancer", 1))) {
      res4.response._2
    }

    // searching for multiple tag that does exist should return the tags (query string case doesn't matter)
    // should be sorted by counts of tags
    val res5 = Await.result(services.workspaceService.getTags(Some("cAn")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("cancer", 1))) {
      res5.response._2
    }

    // searching for with no query should return all tags
    val res6 = Await.result(services.workspaceService.getTags(None), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector(WorkspaceTag("cantaloupe", 2), WorkspaceTag("buffalo", 1), WorkspaceTag("cancer", 1))) {
      res6.response._2
    }

    // remove tags
    Await.result(services.workspaceService.updateWorkspace(testData.wsName, Seq(RemoveAttribute(AttributeName.withTagsNS))), Duration.Inf)
    Await.result(services.workspaceService.updateWorkspace(testData.wsName7, Seq(RemoveAttribute(AttributeName.withTagsNS))), Duration.Inf)


    // make sure that tags no longer exists
    val res7 = Await.result(services.workspaceService.getTags(Some("aNc")), Duration.Inf)
      .asInstanceOf[RequestComplete[(StatusCode, Vector[WorkspaceTag])]]
    assertResult(Vector.empty[WorkspaceTag]) {
      res7.response._2
    }

  }

  for ((policyName, shouldShare) <- Seq((SamWorkspacePolicyNames.writer, false), (SamWorkspacePolicyNames.canCompute, true), (SamWorkspacePolicyNames.reader, false))) {
    it should s"${if (!shouldShare) "not " else ""}share billing compute when workspace $policyName access granted" in withTestDataServicesCustomSam { services =>
      val email = s"${UUID.randomUUID}@bar.com"
      val results = Set((policyName, email))
      Await.result(services.workspaceService.maybeShareProjectComputePolicy(results, testData.workspace.toWorkspaceName), Duration.Inf)

      val expectedPolicyEntry = (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, email)
      if(shouldShare) {
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs(Set(expectedPolicyEntry))
      } else {
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs(Set.empty)
      }
    }
  }

  val aclTestUser = UserInfo(RawlsUserEmail("acl-test-user"), OAuth2BearerToken(""), 0, RawlsUserSubjectId("acl-test-user-subject-id"))

  def allWorkspaceAclUpdatePermutations(emailString: String): Seq[WorkspaceACLUpdate] = for {
    accessLevel <- WorkspaceAccessLevels.all
    canShare <- Set(Some(true), Some(false), None)
    canCompute <- Set(Some(true), Some(false), None)
  } yield WorkspaceACLUpdate(emailString, accessLevel, canShare, canCompute)

  def expectedPolicies(aclUpdate: WorkspaceACLUpdate): Either[StatusCode, Set[(SamResourceTypeName, SamResourcePolicyName)]] = {
    aclUpdate match {
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.ProjectOwner, _, _) => Left(StatusCodes.BadRequest)
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Owner, _, _) => Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.owner))

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, canShare, canCompute) =>
        val canSharePolicy = canShare match {
          case None | Some(false) => Set.empty
          case Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareWriter)
        }
        val canComputePolicy = canCompute match {
          case None | Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.canCompute, SamResourceTypeNames.billingProject -> SamBillingProjectPolicyNames.canComputeUser)
          case Some(false) => Set.empty
        }
        Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.writer) ++ canSharePolicy ++ canComputePolicy)

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, canShare, canCompute) =>
        if (canCompute.contains(true)) {
          Left(StatusCodes.BadRequest)
        } else {
          val canSharePolicy = canShare match {
            case None | Some(false) => Set.empty
            case Some(true) => Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.shareReader)
          }
          Right(Set(SamResourceTypeNames.workspace -> SamWorkspacePolicyNames.reader) ++ canSharePolicy)
        }

      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.NoAccess, _, _) => Right(Set.empty)
    }
  }

  for (aclUpdate <- allWorkspaceAclUpdatePermutations(aclTestUser.userEmail.value)) {
    it should s"add correct policies for $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)
      }

      (expectedPolicies(aclUpdate), result) match {
        case (Left(statusCode), util.Failure(exception: RawlsExceptionWithErrorReport)) => assertResult(Some(statusCode), result.toString) {
          exception.errorReport.statusCode
        }

        case (Right(policies), util.Success(_)) =>
          val expectedAdds = policies.map {
            case (SamResourceTypeNames.workspace, policyName) => (SamResourceTypeNames.workspace, testData.workspace.workspaceId, policyName, aclTestUser.userEmail.value)
            case (SamResourceTypeNames.billingProject, policyName) => (SamResourceTypeNames.billingProject, testData.workspace.namespace, policyName, aclTestUser.userEmail.value)
            case _ => throw new Exception("make the compiler happy")
          }

          withClue(result.toString) {
            services.samDAO.callsToAddToPolicy should contain theSameElementsAs expectedAdds
            services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
          }

        case (_, r) => fail(r.toString)
      }
    }
  }

  it should s"add correct policies for group" in withTestDataServicesCustomSam { services =>
    // setting the email to None is what a group looks like
    services.samDAO.userEmails.put(aclTestUser.userEmail.value, None)
    populateWorkspacePolicies(services)

    val aclUpdate = WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.Write)

    val result = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)

    withClue(result.toString) {
      services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set(
        (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, aclTestUser.userEmail.value),
        (SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCompute, aclTestUser.userEmail.value),
        (SamResourceTypeNames.billingProject, testData.workspace.namespace, SamBillingProjectPolicyNames.canComputeUser, aclTestUser.userEmail.value)
      )
      services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
    }
  }

  it should "not clobber catalog permission" in withTestDataServicesCustomSam { services =>
    populateWorkspacePolicies(services)
    Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)

    val aclUpdate = WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.Write)
    Await.result(services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.canCatalog, SamPolicy(Set(WorkbenchEmail(aclUpdate.email)), Set.empty, Set(SamWorkspaceRoles.canCatalog)), userInfo), Duration.Inf)
    val result = Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)

    withClue(result.toString) {
      services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
    }
  }


  def addEmailToPolicy(services: TestApiServiceWithCustomSamDAO, policyName: SamResourcePolicyName, email: String) = {
    val policy = services.samDAO.policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId))(policyName)
    val updateMembers = policy.policy.memberEmails + WorkbenchEmail(email)
    val updatedPolicy = policy.copy(policy = policy.policy.copy(memberEmails = updateMembers))
    services.samDAO.policies((SamResourceTypeNames.workspace, testData.workspace.workspaceId)).put(policyName, updatedPolicy)
  }

  val testPolicyNames = Set(SamWorkspacePolicyNames.canCompute, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader, SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.projectOwner, SamWorkspacePolicyNames.shareReader, SamWorkspacePolicyNames.shareWriter)
  for(testPolicyName1 <- testPolicyNames; testPolicyName2 <- testPolicyNames if testPolicyName1 != testPolicyName2 && !(testPolicyName1 == SamWorkspacePolicyNames.shareReader && testPolicyName2 == SamWorkspacePolicyNames.shareWriter) && !(testPolicyName1 == SamWorkspacePolicyNames.shareWriter && testPolicyName2 == SamWorkspacePolicyNames.shareReader)) {
    it should s"remove $testPolicyName1 and $testPolicyName2" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName1, aclTestUser.userEmail.value)
      addEmailToPolicy(services, testPolicyName2, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(WorkspaceACLUpdate(aclTestUser.userEmail.value, WorkspaceAccessLevels.NoAccess)), inviteUsersNotFound = false), Duration.Inf)
      }

      if (testPolicyName1 == SamWorkspacePolicyNames.projectOwner || testPolicyName2 == SamWorkspacePolicyNames.projectOwner) {
        val error = intercept[RawlsExceptionWithErrorReport] {
          result.get
        }
        assertResult(Some(StatusCodes.BadRequest), result.toString) {
          error.errorReport.statusCode
        }
      } else {
        assert(result.isSuccess, result.toString)
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set(
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName1, aclTestUser.userEmail.value),
          (SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName2, aclTestUser.userEmail.value)
        )
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      }

    }
  }

  for(testPolicyName <- Set(SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader, SamWorkspacePolicyNames.owner); aclUpdate <- Set(WorkspaceAccessLevels.Read ,WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Owner).map(l => WorkspaceACLUpdate(aclTestUser.userEmail.value, l, canCompute = Some(false)))) {
    it should s"change $testPolicyName to $aclUpdate" in withTestDataServicesCustomSam { services =>
      Await.result(services.samDAO.registerUser(aclTestUser), Duration.Inf)
      populateWorkspacePolicies(services)

      addEmailToPolicy(services, testPolicyName, aclTestUser.userEmail.value)

      val result = Try {
        Await.result(services.workspaceService.updateACL(testData.workspace.toWorkspaceName, Set(aclUpdate), inviteUsersNotFound = false), Duration.Inf)
      }

      assert(result.isSuccess, result.toString)

      if (aclUpdate.accessLevel.toPolicyName.contains(testPolicyName.value)) {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set.empty
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set.empty
      } else {
        services.samDAO.callsToRemoveFromPolicy should contain theSameElementsAs Set((SamResourceTypeNames.workspace, testData.workspace.workspaceId, testPolicyName, aclTestUser.userEmail.value))
        services.samDAO.callsToAddToPolicy should contain theSameElementsAs Set((SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamResourcePolicyName(aclUpdate.accessLevel.toPolicyName.get), aclTestUser.userEmail.value))
      }
    }
  }


  it should "parse workflow metadata" in {
    val jsonString = """{
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

  it should "204 on add linked service accounts to workspace" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete = Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      assertResult(RequestComplete(StatusCodes.NoContent)) {
        rqComplete
      }
    }
  }

  it should "404 on add linked service accounts to workspace which does not exist" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName.copy(name = "DNE")), Duration.Inf)
      }
      assertResult(Some(StatusCodes.NotFound)) {
        error.errorReport.statusCode
      }
    }
  }

  it should "404 on add linked service accounts to workspace with no access" in withTestDataServicesCustomSamAndUser(RawlsUser(RawlsUserSubjectId("no-access"), RawlsUserEmail("no-access"))) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { ctx =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      }
      assertResult(Some(StatusCodes.NotFound)) {
        error.errorReport.statusCode
      }
    }
  }

  it should "403 on add linked service accounts to workspace with read access" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { ctx =>
      val error = intercept[RawlsExceptionWithErrorReport] {
        Await.result(services.workspaceService.enableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      }
      assertResult(Some(StatusCodes.Forbidden)) {
        error.errorReport.statusCode
      }
    }
  }

  it should "204 on remove linked service accounts to workspace" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      assertResult(RequestComplete(StatusCodes.NoContent)) {
        rqComplete
      }
    }
  }

  it should "204 on remove linked service accounts to workspace which does not exist" in withTestDataServices { services =>
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName.copy(name = "DNE")), Duration.Inf)
      assertResult(RequestComplete(StatusCodes.NoContent)) {
        rqComplete
      }
    }
  }

  it should "204 on remove linked service accounts to workspace with no access" in withTestDataServicesCustomSamAndUser(RawlsUser(RawlsUserSubjectId("no-access"), RawlsUserEmail("no-access"))) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      assertResult(RequestComplete(StatusCodes.NoContent)) {
        rqComplete
      }
    }
  }

  it should "204 on remove linked service accounts to workspace with read access" in withTestDataServicesCustomSamAndUser(testData.userReader) { services =>
    populateWorkspacePolicies(services)
    withWorkspaceContext(testData.workspace) { ctx =>
      val rqComplete = Await.result(services.workspaceService.disableRequesterPaysForLinkedSAs(testData.workspace.toWorkspaceName), Duration.Inf)
      assertResult(RequestComplete(StatusCodes.NoContent)) {
        rqComplete
      }
    }
  }

}
