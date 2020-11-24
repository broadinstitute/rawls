package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.PoisonPill
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.google.api.services.cloudresourcemanager.model.Project
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.{DataRepoEntityProviderConfig, DeploymentManagerConfig, MethodRepoConfig, WorkspaceServiceConfig}
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.{MockGoogleAccessContextManagerDAO, MockGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.mock.{MockBondApiDAO, MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.{Agora, CreationStatuses, Dockstore, GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsUser, ServicePerimeterName, UserInfo, Workspace, WorkspaceName, WorkspaceRequest, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.{MethodConfigApiService, SubmissionApiService, WorkspaceApiService}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito
import org.mockito.Mockito.{RETURNS_SMART_NULLS, verify, when}
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class WorkspaceCreatorSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest with MockitoTestUtils with TestDriverComponent with OptionValues with BeforeAndAfterAll {
  protected val mockServer: RemoteServicesMockServer = RemoteServicesMockServer()
  protected val workbenchMetricBaseName = "test"

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectivesWithUser {
    private val userInfo1 = UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId)
    lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(userInfo1)
    lazy val userService: UserService = userServiceConstructor(userInfo1)


    def actorRefFactory = system

    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val googleAccessContextManagerDAO = Mockito.spy(new MockGoogleAccessContextManagerDAO())
    val gcsDAO = Mockito.spy(new MockGoogleServicesDAO("test", googleAccessContextManagerDAO))
    val samDAO = new MockSamDAO(dataSource)
    val gpsDAO = new MockGooglePubSubDAO
    val workspaceManagerDAO = mock[MockWorkspaceManagerDAO](RETURNS_SMART_NULLS)
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
      "requesterPaysRole",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate"))
    ) _

    val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    ) _

    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService("test", "test", 31, bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-",
      Map(ServicePerimeterName("theGreatBarrier") -> Seq(GoogleProjectNumber("555555"), GoogleProjectNumber("121212")),
        ServicePerimeterName("anotherGoodName") -> Seq(GoogleProjectNumber("777777"), GoogleProjectNumber("343434")))
    )

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService = new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()
    val entityManager = EntityManager.defaultEntityManager(dataSource, workspaceManagerDAO, dataRepoDAO, samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10, 0))

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
    ) _

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withTestDataServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }
  }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
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


  // TODO: This test will need to be deleted when implementing https://broadworkbench.atlassian.net/browse/CA-947
  it should "succeed regardless of the BillingProject.status" in withTestDataServices { services =>
    CreationStatuses.all.foreach { projectStatus =>
      // Update the BillingProject with the CreationStatus under test
      runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.testProject1.copy(status = projectStatus))))

      // Create a Workspace in the BillingProject
      val workspaceName = WorkspaceName(testData.testProject1Name.value, s"ws_with_status_${projectStatus}")
      val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

      // Load the newly created Workspace for assertions
      val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
      maybeWorkspace.value.name shouldBe workspaceName.name
    }
  }

  it should "fail with 400 if specified Namespace/Billing Project does not exist" in withTestDataServices { services =>
    val workspaceRequest = WorkspaceRequest("nonexistent_namespace", "kermits_pond", Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail with 500 if Billing Project does not have a Billing Account specified" in withTestDataServices { services =>
    // Update BillingProject to wipe BillingAccount field.  Reload BillingProject and confirm that field is empty
    runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.testProject1.copy(billingAccount = None))))
    val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    updatedBillingProject.value.billingAccount shouldBe empty

    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "banana_palooza", Map.empty)
    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }
    error.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
  }

  it should "fail with 403 and set the invalidBillingAcct field if Rawls does not have the required IAM permissions on the Google Billing Account" in withTestDataServices { services =>
    // Preconditions: setup the BillingProject to have the BillingAccountName that will "fail" the permissions check in
    // the MockGoogleServicesDAO.  Then confirm that the BillingProject.invalidBillingAccount field starts as FALSE

    val billingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")
    runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.testProject1.copy(billingAccount = Option(billingAccountName)))))
    val originalBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    originalBillingProject.value.invalidBillingAccount shouldBe false

    // Make the call to createWorkspace and make sure it throws an exception with the correct StatusCode
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "whatever", Map.empty)
    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }
    error.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

    // Make sure that the BillingProject.invalidBillingAccount field was properly updated while attempting to create the
    // Workspace
    val persistedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    persistedBillingProject.value.invalidBillingAccount shouldBe true
  }

  it should "fail with 502 if Rawls is unable to retrieve the Google Project Number from Google for Workspace's Google Project" in withTestDataServices { services =>
    when(services.gcsDAO.getGoogleProject(any[GoogleProjectId])).thenReturn(Future.successful(new Project().setProjectNumber(null)))

    val workspaceName = WorkspaceName(testData.testProject1Name.value, "whatever")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadGateway)

    val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
    maybeWorkspace shouldBe None
  }

  it should "set the Billing Account on the Workspace's Google Project to match the Billing Project's Billing Account" in withTestDataServices { services =>
    val billingProject = testData.testProject1
    val workspaceName = WorkspaceName(billingProject.projectName.value, "cool_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    // Project ID gets allocated when creating the Workspace, so we don't care what it is here.  We do care that
    // whatever that Google Project is, we set the right Billing Account on it, which is the Billing Account specified
    // in the Billing Project
    val billingAccountNameCaptor = captor[RawlsBillingAccountName]
    verify(services.gcsDAO).setBillingAccountForProject(any[GoogleProjectId], billingAccountNameCaptor.capture, anyBoolean())
    billingAccountNameCaptor.getValue shouldEqual billingProject.billingAccount.get
  }

  it should "throw an exception when calling GoogleServicesDAO to update the Billing Account on the Workspace's Google Project and the DAO call fails" in withTestDataServices { services =>
    when(services.gcsDAO.setBillingAccountForProject(any[GoogleProjectId], any[RawlsBillingAccountName], anyBoolean()))
      .thenReturn(Future.failed(new Exception("Fake error from Google")))

    val workspaceName = WorkspaceName(testData.testProject1Name.value, "sad_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    intercept[Exception] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
    maybeWorkspace shouldBe None
  }

  it should "not try to modify the Service Perimeter if the Billing Project does not specify a Service Perimeter" in withTestDataServices { services =>
    val newWorkspaceName = "space_for_workin"
    val billingProject = testData.testProject1

    // Pre-condition: make sure that the Billing Project we're adding the Workspace to DOES NOT specify a Service
    // Perimeter
    billingProject.servicePerimeter shouldBe empty

    val workspaceRequest = WorkspaceRequest(billingProject.projectName.value, newWorkspaceName, Map.empty)
    Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    // Verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was NOT called
    verify(services.googleAccessContextManagerDAO, Mockito.never()).overwriteProjectsInServicePerimeter(any[ServicePerimeterName], any[Seq[String]])
  }

  it should "claim a Google Project from Resource Buffering Service" in pending

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "creating a Workspace in a Service Perimeter" should "attempt to overwrite the Service Perimeter with the correct list of Google Project Numbers" in withTestDataServices { services =>
    // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
    val servicePerimeterName: ServicePerimeterName = services.workspaceServiceConfig.staticProjectsInPerimeters.keys.head
    val staticProjectNumbersInPerimeter: Seq[String] = services.workspaceServiceConfig.staticProjectsInPerimeters(servicePerimeterName).map(_.value)

    val billingProject1 = testData.testProject1
    val billingProject2 = testData.testProject2
    val billingProjects = Seq(billingProject1, billingProject2)
    val workspacesPerProject = 2

    // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
    // the Billing Projects and therefore in the Perimeter
    val workspacesInPerimeter: Seq[Workspace] = billingProjects.flatMap { bp =>
      runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(bp.copy(servicePerimeter = Option(servicePerimeterName)))))
      val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName))
      updatedBillingProject.value.servicePerimeter.value shouldBe servicePerimeterName

      (1 to workspacesPerProject).map { n =>
        val workspace = testData.workspace.copy(
          namespace = bp.projectName.value,
          name = s"${bp.projectName.value}Workspace${n}",
          workspaceId = UUID.randomUUID().toString,
          googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString)))
        runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
      }
    }

    // Test setup is done, now we're getting to the test
    // Make a call to Create a new Workspace in the same Billing Project
    val workspaceName = WorkspaceName(testData.testProject1Name.value, "cool_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
    val workspace = Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)

    // Check that we made the call to overwrite the Perimeter exactly once (default) and that the correct perimeter
    // name was specified with the correct list of projects which should include all pre-existing Workspaces within
    // Billing Projects using the same Service Perimeter, all static Google Project Numbers specified by the Config, and
    // the new Google Project Number that we just created
    val existingProjectNumbersInPerimeter = workspacesInPerimeter.map(_.googleProjectNumber.get.value)
    val expectedGoogleProjectNumbers: Seq[String] = (existingProjectNumbersInPerimeter ++ staticProjectNumbersInPerimeter) :+ workspace.googleProjectNumber.get.value
    val projectNumbersCaptor = captor[Seq[String]]
    val servicePerimeterNameCaptor = captor[ServicePerimeterName]
    // verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was called exactly once and capture
    // the arguments passed to it so that we can verify that they were correct
    verify(services.googleAccessContextManagerDAO).overwriteProjectsInServicePerimeter(servicePerimeterNameCaptor.capture, projectNumbersCaptor.capture)
    projectNumbersCaptor.getValue should contain theSameElementsAs expectedGoogleProjectNumbers
    servicePerimeterNameCaptor.getValue shouldBe servicePerimeterName
  }

  "cloneWorkspace" should "create a V2 Workspace" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "cloned_space"
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, newWorkspaceName, Map.empty)

    val workspace = Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)

    workspace.name should be(newWorkspaceName)
    workspace.workspaceVersion should be(WorkspaceVersions.V2)
    workspace.googleProjectId.value should not be empty
    workspace.googleProjectNumber should not be empty
  }

  it should "fail with 400 if specified Namespace/Billing Project does not exist" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest("nonexistent_namespace", "kermits_pond", Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "fail with 500 if Billing Project does not have a Billing Account specified" in withTestDataServices { services =>
    // Update BillingProject to wipe BillingAccount field.  Reload BillingProject and confirm that field is empty
    runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.testProject1.copy(billingAccount = None))))
    val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    updatedBillingProject.value.billingAccount shouldBe empty

    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "banana_palooza", Map.empty)
    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)
    }
    error.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
  }

  it should "fail with 403 and set the invalidBillingAcct field if Rawls does not have the required IAM permissions on the Google Billing Account" in withTestDataServices { services =>
    // Preconditions: setup the BillingProject to have the BillingAccountName that will "fail" the permissions check in
    // the MockGoogleServicesDAO.  Then confirm that the BillingProject.invalidBillingAccount field starts as FALSE
    val billingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")
    runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(testData.testProject1.copy(billingAccount = Option(billingAccountName)))))
    val originalBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    originalBillingProject.value.invalidBillingAccount shouldBe false

    // Make the call to createWorkspace and make sure it throws an exception with the correct StatusCode
    val baseWorkspace = testData.workspace
    val workspaceRequest = WorkspaceRequest(testData.testProject1Name.value, "whatever", Map.empty)
    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)
    }
    error.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)

    // Make sure that the BillingProject.invalidBillingAccount field was properly updated while attempting to create the
    // Workspace
    val persistedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(testData.testProject1Name))
    persistedBillingProject.value.invalidBillingAccount shouldBe true
  }

  it should "fail with 502 if Rawls is unable to retrieve the Google Project Number from Google for Workspace's Google Project" in withTestDataServices { services =>
    when(services.gcsDAO.getGoogleProject(any[GoogleProjectId])).thenReturn(Future.successful(new Project().setProjectNumber(null)))

    val workspaceName = WorkspaceName(testData.testProject1Name.value, "whatever")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    val error: RawlsExceptionWithErrorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(services.workspaceService.createWorkspace(workspaceRequest), Duration.Inf)
    }

    error.errorReport.statusCode shouldBe Some(StatusCodes.BadGateway)

    val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
    maybeWorkspace shouldBe None
  }

  it should "set the Billing Account on the Workspace's Google Project to match the Billing Project's Billing Account" in withTestDataServices { services =>
    val billingProject = testData.testProject1
    val workspaceName = WorkspaceName(billingProject.projectName.value, "cool_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    val baseWorkspace = testData.workspace
    Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)

    // Project ID gets allocated when creating the Workspace, so we don't care what it is here.  We do care that
    // whatever that Google Project is, we set the right Billing Account on it, which is the Billing Account specified
    // in the Billing Project
    val billingAccountNameCaptor = captor[RawlsBillingAccountName]
    verify(services.gcsDAO).setBillingAccountForProject(any[GoogleProjectId], billingAccountNameCaptor.capture, anyBoolean())
    billingAccountNameCaptor.getValue shouldEqual billingProject.billingAccount.get
  }

  it should "throw an exception when calling GoogleServicesDAO to update the Billing Account on the Workspace's Google Project and the DAO call fails" in withTestDataServices { services =>
    when(services.gcsDAO.setBillingAccountForProject(any[GoogleProjectId], any[RawlsBillingAccountName], anyBoolean()))
      .thenReturn(Future.failed(new Exception("Fake error from Google")))

    val baseWorkspace = testData.workspace
    val workspaceName = WorkspaceName(testData.testProject1Name.value, "sad_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)

    intercept[Exception] {
      Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)
    }

    val maybeWorkspace = runAndWait(workspaceQuery.findByName(workspaceName))
    maybeWorkspace shouldBe None
  }

  it should "not try to modify the Service Perimeter if the Billing Project does not specify a Service Perimeter" in withTestDataServices { services =>
    val baseWorkspace = testData.workspace
    val newWorkspaceName = "space_for_workin"
    val billingProject = testData.testProject1

    // Pre-condition: make sure that the Billing Project we're adding the Workspace to DOES NOT specify a Service
    // Perimeter
    billingProject.servicePerimeter shouldBe empty

    val workspaceRequest = WorkspaceRequest(billingProject.projectName.value, newWorkspaceName, Map.empty)
    Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)

    // Verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was NOT called
    verify(services.googleAccessContextManagerDAO, Mockito.never()).overwriteProjectsInServicePerimeter(any[ServicePerimeterName], any[Seq[String]])
  }

  it should "claim a Google Project from Resource Buffering Service" in pending

  // There is another test in WorkspaceComponentSpec that gets into more scenarios for selecting the right Workspaces
  // that should be within a Service Perimeter
  "cloning a Workspace into a Service Perimeter" should "attempt to overwrite the Service Perimeter with the correct list of Google Project Numbers" in withTestDataServices { services =>
    // Use the WorkspaceServiceConfig to determine which static projects exist for which perimeter
    val servicePerimeterName: ServicePerimeterName = services.workspaceServiceConfig.staticProjectsInPerimeters.keys.head
    val staticProjectNumbersInPerimeter: Seq[String] = services.workspaceServiceConfig.staticProjectsInPerimeters(servicePerimeterName).map(_.value)

    val billingProject1 = testData.testProject1
    val billingProject2 = testData.testProject2
    val billingProjects = Seq(billingProject1, billingProject2)
    val workspacesPerProject = 2

    // Setup BillingProjects by updating their Service Perimeter fields, then pre-populate some Workspaces in each of
    // the Billing Projects and therefore in the Perimeter
    val workspacesInPerimeter: Seq[Workspace] = billingProjects.flatMap { bp =>
      runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(bp.copy(servicePerimeter = Option(servicePerimeterName)))))
      val updatedBillingProject = runAndWait(slickDataSource.dataAccess.rawlsBillingProjectQuery.load(bp.projectName))
      updatedBillingProject.value.servicePerimeter.value shouldBe servicePerimeterName

      (1 to workspacesPerProject).map { n =>
        val workspace = testData.workspace.copy(
          namespace = bp.projectName.value,
          name = s"${bp.projectName.value}Workspace${n}",
          workspaceId = UUID.randomUUID().toString,
          googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString)))
        runAndWait(slickDataSource.dataAccess.workspaceQuery.createOrUpdate(workspace))
      }
    }

    // Test setup is done, now we're getting to the test
    // Make a call to Create a new Workspace in the same Billing Project
    val baseWorkspace = testData.workspace
    val workspaceName = WorkspaceName(testData.testProject1Name.value, "cool_workspace")
    val workspaceRequest = WorkspaceRequest(workspaceName.namespace, workspaceName.name, Map.empty)
    val workspace = Await.result(services.workspaceService.cloneWorkspace(baseWorkspace.toWorkspaceName, workspaceRequest), Duration.Inf)

    // Check that we made the call to overwrite the Perimeter exactly once (default) and that the correct perimeter
    // name was specified with the correct list of projects which should include all pre-existing Workspaces within
    // Billing Projects using the same Service Perimeter, all static Google Project Numbers specified by the Config, and
    // the new Google Project Number that we just created
    val existingProjectNumbersInPerimeter = workspacesInPerimeter.map(_.googleProjectNumber.get.value)
    val expectedGoogleProjectNumbers: Seq[String] = (existingProjectNumbersInPerimeter ++ staticProjectNumbersInPerimeter) :+ workspace.googleProjectNumber.get.value
    val projectNumbersCaptor = captor[Seq[String]]
    val servicePerimeterNameCaptor = captor[ServicePerimeterName]
    // verify that googleAccessContextManagerDAO.overwriteProjectsInServicePerimeter was called exactly once and capture
    // the arguments passed to it so that we can verify that they were correct
    verify(services.googleAccessContextManagerDAO).overwriteProjectsInServicePerimeter(servicePerimeterNameCaptor.capture, projectNumbersCaptor.capture)
    projectNumbersCaptor.getValue should contain theSameElementsAs expectedGoogleProjectNumbers
    servicePerimeterNameCaptor.getValue shouldBe servicePerimeterName
  }
}
