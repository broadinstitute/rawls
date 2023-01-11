package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.model.{IamRole, RoleBinding, RoleBindingList}
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{
  NoSuchWorkspaceException,
  RawlsExceptionWithErrorReport,
  UserDisabledException,
  WorkspaceAccessDeniedException
}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.rawls.QueryMatcher
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import spray.json.{JsObject, JsString}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.reflect.runtime.universe.typeOf

/**
  * Unit tests kept separate from WorkspaceServiceSpec to separate true unit tests from tests requiring external resources
  */
class WorkspaceServiceUnitTests extends AnyFlatSpec with OptionValues with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val workspace = Workspace(
    "test-namespace",
    "test-name",
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  def workspaceServiceConstructor(
    datasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS),
    methodRepoDAO: MethodRepoDAO = mock[MethodRepoDAO](RETURNS_SMART_NULLS),
    cromiamDAO: ExecutionServiceDAO = mock[ExecutionServiceDAO](RETURNS_SMART_NULLS),
    executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster](RETURNS_SMART_NULLS),
    execServiceBatchSize: Int = 1,
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
    methodConfigResolver: MethodConfigResolver = mock[MethodConfigResolver](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO](RETURNS_SMART_NULLS),
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS),
    genomicsServiceConstructor: RawlsRequestContext => GenomicsService = _ =>
      mock[GenomicsService](RETURNS_SMART_NULLS),
    maxActiveWorkflowsTotal: Int = 1,
    maxActiveWorkflowsPerUser: Int = 1,
    workbenchMetricBaseName: String = "",
    submissionCostService: SubmissionCostService = mock[SubmissionCostService](RETURNS_SMART_NULLS),
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig](RETURNS_SMART_NULLS),
    requesterPaysSetupService: RequesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS),
    entityManager: EntityManager = mock[EntityManager](RETURNS_SMART_NULLS),
    resourceBufferService: ResourceBufferService = mock[ResourceBufferService](RETURNS_SMART_NULLS),
    resourceBufferSaEmail: String = "",
    servicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS),
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO](RETURNS_SMART_NULLS),
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = "",
    billingProfileManagerDAO: BillingProfileManagerDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS),
    aclManagerDatasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS)
  ): RawlsRequestContext => WorkspaceService = info =>
    WorkspaceService.constructor(
      datasource,
      methodRepoDAO,
      cromiamDAO,
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
      config,
      requesterPaysSetupService,
      entityManager,
      resourceBufferService,
      resourceBufferSaEmail,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      new RawlsWorkspaceAclManager(samDAO),
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, aclManagerDatasource)
    )(info)(mock[Materializer], scala.concurrent.ExecutionContext.global)

  "getWorkspaceById" should "return the workspace returned by getWorkspace(WorkspaceName) on success" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List(("abc", "cba"))))

    val service = spy(workspaceServiceConstructor(datasource)(defaultRequestContext))
    // Note that getWorkspaceById doesn't do any processing to a successful value at all
    // it will pass on literally any valid JsObject returned by getWorkspace
    val expected = new JsObject(Map("dummyKey" -> JsString("dummyVal")))

    doReturn(Future.successful(expected))
      .when(service)
      .getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())

    val result = Await.result(service.getWorkspaceById("c1e14bc7-cc7f-4710-a383-74370be3cba1", WorkspaceFieldSpecs()),
                              Duration.Inf
    )
    assertResult(expected)(result)
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
  }

  it should "return the exception thrown by getWorkspace(WorkspaceName) on failure" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List(("abc", "cba"))))

    val service = spy(workspaceServiceConstructor(datasource)(defaultRequestContext))
    val exception =
      new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "A generic exception"))
    doReturn(Future.failed(exception))
      .when(service)
      .getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspaceById("c1e14bc7-cc7f-4710-a383-74370be3cba1", WorkspaceFieldSpecs()),
                   Duration.Inf
      )
    }

    assertResult(exception)(result)
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
  }

  it should "return an exception without the workspace name when getWorkspace(WorkspaceName) is not found" in {
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "123")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = spy(workspaceServiceConstructor(datasource)(defaultRequestContext))

    doReturn(Future.failed(NoSuchWorkspaceException(WorkspaceName("abc", "123"))))
      .when(service)
      .getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())

    val workspaceId = "c1e14bc7-cc7f-4710-a383-74370be3cba1"
    val exception = intercept[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceById(workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }
    assert(exception.workspace == workspaceId)
    assert(!exception.getMessage.contains("abc"))
    assert(!exception.getMessage.contains("123"))
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())
  }

  it should "return an exception without the workspace name when getWorkspace(WorkspaceName) fails access checks" in {
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "123")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)

    val service = spy(workspaceServiceConstructor(datasource)(defaultRequestContext))
    doReturn(Future.failed(WorkspaceAccessDeniedException(WorkspaceName("abc", "123"))))
      .when(service)
      .getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())

    val workspaceId = "c1e14bc7-cc7f-4710-a383-74370be3cba1"
    val exception = intercept[WorkspaceAccessDeniedException] {
      Await.result(service.getWorkspaceById(workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    assert(exception.workspace == workspaceId)
    assert(!exception.getMessage.contains("abc"))
    assert(!exception.getMessage.contains("123"))
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())
  }

  it should "return an exception with the workspaceId when no workspace is found in the initial query" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List()))

    val workspaceId = "c1e14bc7-cc7f-4710-a383-74370be3cba1"

    val exception = intercept[NoSuchWorkspaceException] {
      val service = workspaceServiceConstructor(datasource)(defaultRequestContext)
      Await.result(service.getWorkspaceById(workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    assert(exception.workspace == workspaceId)
  }

  it should "return an unauthorized error if the user is disabled" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List()))
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val samUserStatus = SamUserStatusResponse("sub", "email", enabled = false)
    when(samDAO.getUserStatus(ArgumentMatchers.eq(defaultRequestContext))).thenReturn(
      Future.successful(Some(samUserStatus))
    )

    val exception = intercept[UserDisabledException] {
      val service = workspaceServiceConstructor(datasource, samDAO = samDAO)(defaultRequestContext)
      Await.result(service.getWorkspace(WorkspaceName("fake_namespace", "fake_name"), WorkspaceFieldSpecs()),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.Unauthorized)
  }

  "assertNoGoogleChildrenBlockingWorkspaceDeletion" should "not error if the only child is the google project" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq()))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf) shouldBe ()
  }

  it should "error if the workspace google project has a child resource" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Seq()))
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 1
  }

  it should "error if the workspace has a child resource besides it's google project" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 1
  }

  it should "return an error for each blocking child resource in the error report" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value),
            SamFullyQualifiedResourceId("another-resource", SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 2
  }

  it should "error if there is no googleProjectId" in {
    val samDAO = mock[SamDAO]
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)
    val wsId = UUID.randomUUID().toString
    val azureWorkspace = Workspace.buildMcWorkspace(
      namespace = "test-azure-bp",
      name = s"test-azure-ws-${wsId}",
      workspaceId = wsId,
      createdDate = DateTime.now,
      lastModified = DateTime.now,
      createdBy = "testuser@example.com",
      attributes = Map()
    )

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(azureWorkspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.InternalServerError
    assert(error.errorReport.message contains "with no googleProjectId")
  }

  def mockWsmForAclTests(ownerEmail: String = "owner@example.com",
                         writerEmail: String = "writer@example.com",
                         readerEmail: String = "reader@example.com"
  ): WorkspaceManagerDAO = {
    val ownerBinding = new RoleBinding().role(IamRole.OWNER).members(List(ownerEmail).asJava)
    val writerBinding = new RoleBinding().role(IamRole.WRITER).members(List(writerEmail).asJava)
    val readerBinding = new RoleBinding().role(IamRole.READER).members(List(readerEmail).asJava)
    val discovererBinding =
      new RoleBinding().role(IamRole.DISCOVERER).members(List("discoverer@example.com", readerEmail).asJava)
    val applicationBinding = new RoleBinding().role(IamRole.APPLICATION).members(List("application@example.com").asJava)
    val wsmRoleBindings = new RoleBindingList()
    wsmRoleBindings.addAll(
      List(ownerBinding, writerBinding, readerBinding, discovererBinding, applicationBinding).asJava
    )
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDAO.getRoles(any(), any())).thenReturn(wsmRoleBindings)
    wsmDAO
  }

  def mockSamForAclTests(): SamDAO = {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserIdInfo(any(), any())).thenReturn(
      Future.successful(SamDAO.User(UserIdInfo("fake_user_id", "user@example.com", Option("fake_google_subject_id"))))
    )
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    samDAO
  }

  def mockDatasourceForAclTests(workspaceType: WorkspaceType,
                                workspaceId: UUID = UUID.randomUUID()
  ): SlickDataSource = {
    val datasource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    val googleProjectId = workspaceType match {
      case WorkspaceType.McWorkspace    => GoogleProjectId("")
      case WorkspaceType.RawlsWorkspace => GoogleProjectId("fake-project-id")
    }

    when(datasource.inTransaction[Workspace](any(), any())).thenReturn(
      Future.successful(
        Workspace("fake_namespace",
                  "fake_name",
                  workspaceId.toString,
                  "fake_bucket",
                  None,
                  DateTime.now(),
                  DateTime.now(),
                  "creator@example.com",
                  Map.empty
        ).copy(workspaceType = workspaceType, googleProjectId = googleProjectId)
      )
    )
    datasource
  }

  def samWorkspacePoliciesForAclTests(ownerEmail: String,
                                      writerEmail: String,
                                      readerEmail: String
  ): Set[SamPolicyWithNameAndEmail] = Set(
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.owner,
                              SamPolicy(Set(WorkbenchEmail(ownerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("ownerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer,
                              SamPolicy(Set(WorkbenchEmail(writerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("writerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.reader,
                              SamPolicy(Set(WorkbenchEmail(readerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("readerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareWriter,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("shareWriterPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCompute,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("canComputePolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareReader,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("shareReaderPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("projectOwnerPolicy@example.com")
    )
  )

  "getAcl" should "fetch policies from Sam for Rawls workspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(ownerEmail, writerEmail, readerEmail))
    )

    val datasource = mockDatasourceForAclTests(WorkspaceType.RawlsWorkspace)

    val service = workspaceServiceConstructor(datasource, samDAO = samDAO)(defaultRequestContext)
    val result = Await.result(service.getACL(WorkspaceName("fake_namespace", "fake_name")), Duration.Inf)

    val expected = WorkspaceACL(
      Map(
        ownerEmail -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
        writerEmail -> AccessEntry(WorkspaceAccessLevels.Write, false, false, false),
        readerEmail -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
      )
    )

    result shouldBe expected
    verify(samDAO).listPoliciesForResource(any(), any(), any())
  }

  it should "fetch policies from WSM for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)

    val datasource = mockDatasourceForAclTests(WorkspaceType.McWorkspace)

    val samDAO = mockSamForAclTests()
    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, workspaceManagerDAO = wsmDAO)(defaultRequestContext)

    val expected = WorkspaceACL(
      Map(
        ownerEmail -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
        writerEmail -> AccessEntry(WorkspaceAccessLevels.Write, false, false, false),
        readerEmail -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
      )
    )

    val result = Await.result(service.getACL(WorkspaceName("fake_namespace", "fake_name")), Duration.Inf)

    result shouldBe expected
    verify(samDAO, never).listPoliciesForResource(any(), any(), any())
    verify(wsmDAO).getRoles(any(), any())
  }

  "updateAcl" should "call Sam for Rawls workspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())
    when(samDAO.removeUserFromPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val datasource = mockDatasourceForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)

    val requesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))
    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, requesterPaysSetupService = requesterPaysSetupService)(
        defaultRequestContext
      )

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.NoAccess, Option(false), Option(false)),
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Write, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)

    verify(samDAO).addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                   any(),
                                   ArgumentMatchers.eq(SamWorkspacePolicyNames.writer),
                                   ArgumentMatchers.eq(readerEmail),
                                   any()
    )
    verify(samDAO).removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                        any(),
                                        ArgumentMatchers.eq(SamWorkspacePolicyNames.reader),
                                        ArgumentMatchers.eq(readerEmail),
                                        any()
    )
    verify(samDAO).removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                        any(),
                                        ArgumentMatchers.eq(SamWorkspacePolicyNames.writer),
                                        ArgumentMatchers.eq(writerEmail),
                                        any()
    )
  }

  it should "call WSM for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val datasource = mockDatasourceForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclManagerDatasource = mock[SlickDataSource]
    when(aclManagerDatasource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            RawlsBillingProjectName("fake_namespace"),
            CreationStatuses.Ready,
            None,
            None,
            billingProfileId = Option(UUID.randomUUID().toString)
          )
        )
      )
    )

    val service =
      workspaceServiceConstructor(datasource,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO,
                                  aclManagerDatasource = aclManagerDatasource
      )(defaultRequestContext)

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.NoAccess, Option(false), Option(false)),
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Write, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)

    verify(samDAO, never).addUserToPolicy(any(), any(), any(), any(), any())
    verify(samDAO, never).removeUserFromPolicy(any(), any(), any(), any(), any())
    verify(wsmDAO).removeRole(ArgumentMatchers.eq(workspaceId),
                              ArgumentMatchers.eq(WorkbenchEmail(writerEmail)),
                              ArgumentMatchers.eq(IamRole.WRITER),
                              any()
    )
    verify(wsmDAO).removeRole(ArgumentMatchers.eq(workspaceId),
                              ArgumentMatchers.eq(WorkbenchEmail(readerEmail)),
                              ArgumentMatchers.eq(IamRole.READER),
                              any()
    )
    verify(wsmDAO).grantRole(ArgumentMatchers.eq(workspaceId),
                             ArgumentMatchers.eq(WorkbenchEmail(readerEmail)),
                             ArgumentMatchers.eq(IamRole.WRITER),
                             any()
    )
  }

  it should "not allow share writers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val datasource = mockDatasourceForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, workspaceManagerDAO = wsmDAO)(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow share readers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val datasource = mockDatasourceForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, workspaceManagerDAO = wsmDAO)(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow compute writers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val datasource = mockDatasourceForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(false), Option(true))
    )

    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, workspaceManagerDAO = wsmDAO)(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }
}
