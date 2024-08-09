package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.{IamRole, RoleBinding, RoleBindingList}
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.BucketInfo.LifecycleRule.{LifecycleAction, LifecycleCondition}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.fastpass.FastPassServiceImpl
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{
  GcpBucketLifecycleAction,
  GcpBucketLifecycleCondition,
  GcpBucketLifecycleConfig,
  GcpBucketLifecycleRule
}
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferServiceImpl
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterServiceImpl
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
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{contain, include}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import spray.json.{JsObject, JsString}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

/**
  * Unit tests kept separate from WorkspaceServiceSpec to separate true unit tests from tests requiring external resources
  */
class WorkspaceServiceUnitTests extends AnyFlatSpec with OptionValues with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val workspace: Workspace = Workspace(
    "test-namespace",
    "test-name",
    UUID.randomUUID().toString,
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  def workspaceServiceConstructor(
    datasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS),
    executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster](RETURNS_SMART_NULLS),
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
    leonardoService: LeonardoService = mock[LeonardoService](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO](RETURNS_SMART_NULLS),
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS),
    workbenchMetricBaseName: String = "",
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig](RETURNS_SMART_NULLS),
    requesterPaysSetupService: RequesterPaysSetupServiceImpl = mock[RequesterPaysSetupServiceImpl](RETURNS_SMART_NULLS),
    resourceBufferService: ResourceBufferServiceImpl = mock[ResourceBufferServiceImpl](RETURNS_SMART_NULLS),
    servicePerimeterService: ServicePerimeterServiceImpl = mock[ServicePerimeterServiceImpl](RETURNS_SMART_NULLS),
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO](RETURNS_SMART_NULLS),
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = "",
    terraBucketReaderRole: String = "",
    terraBucketWriterRole: String = "",
    billingProfileManagerDAO: BillingProfileManagerDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS),
    aclManagerDatasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS),
    fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPassServiceImpl = (_, _) =>
      mock[FastPassServiceImpl](RETURNS_SMART_NULLS),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    billingRepository: BillingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
  ): RawlsRequestContext => WorkspaceService = info =>
    new WorkspaceService(
      info,
      datasource,
      executionServiceCluster,
      workspaceManagerDAO,
      leonardoService,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      workbenchMetricBaseName,
      config,
      requesterPaysSetupService,
      resourceBufferService,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      new RawlsWorkspaceAclManager(samDAO),
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, aclManagerDatasource),
      fastPassServiceConstructor,
      workspaceRepository,
      billingRepository
    )(scala.concurrent.ExecutionContext.global)

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
    val azureWorkspace = Workspace.buildReadyMcWorkspace(
      namespace = "test-azure-bp",
      name = s"test-azure-ws-$wsId",
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
    val projectOwnerBinding =
      new RoleBinding().role(IamRole.PROJECT_OWNER).members(List("projectOwner@example.com").asJava)
    val ownerBinding = new RoleBinding().role(IamRole.OWNER).members(List(ownerEmail).asJava)
    val writerBinding = new RoleBinding().role(IamRole.WRITER).members(List(writerEmail).asJava)
    val readerBinding = new RoleBinding().role(IamRole.READER).members(List(readerEmail).asJava)
    val discovererBinding =
      new RoleBinding().role(IamRole.DISCOVERER).members(List("discoverer@example.com", readerEmail).asJava)
    val applicationBinding = new RoleBinding().role(IamRole.APPLICATION).members(List("application@example.com").asJava)
    val wsmRoleBindings = new RoleBindingList()
    wsmRoleBindings.addAll(
      List(projectOwnerBinding,
           ownerBinding,
           writerBinding,
           readerBinding,
           discovererBinding,
           applicationBinding
      ).asJava
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

  def mockWorkspaceRepositoryForAclTests(workspaceType: WorkspaceType,
                                         workspaceId: UUID = UUID.randomUUID()
  ): WorkspaceRepository = {
    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    val googleProjectId = workspaceType match {
      case WorkspaceType.McWorkspace    => GoogleProjectId("")
      case WorkspaceType.RawlsWorkspace => GoogleProjectId("fake-project-id")
    }

    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(
      Future.successful(
        Option(
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
    )
    workspaceRepository
  }

  def samWorkspacePoliciesForAclTests(projectOwnerEmail: String,
                                      ownerEmail: String,
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
    SamPolicyWithNameAndEmail(
      SamWorkspacePolicyNames.projectOwner,
      SamPolicy(Set(WorkbenchEmail(projectOwnerEmail)), Set.empty, Set.empty),
      WorkbenchEmail("projectOwnerPolicy@example.com")
    )
  )

  "getAcl" should "fetch policies from Sam for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )

    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace)

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository, samDAO = samDAO)(defaultRequestContext)
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

    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace)

    val samDAO = mockSamForAclTests()
    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)

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
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())
    when(samDAO.removeUserFromPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)

    val requesterPaysSetupService = mock[RequesterPaysSetupServiceImpl](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service =
      workspaceServiceConstructor(
        workspaceRepository = workspaceRepository,
        samDAO = samDAO,
        requesterPaysSetupService = requesterPaysSetupService,
        fastPassServiceConstructor = (_, _) => mockFastPassService
      )(
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
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
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

    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(
        Future.successful(
        )
      )
    val service =
      workspaceServiceConstructor(
        workspaceRepository = workspaceRepository,
        samDAO = samDAO,
        workspaceManagerDAO = wsmDAO,
        aclManagerDatasource = aclManagerDatasource,
        fastPassServiceConstructor = (_, _) => mockFastPassService
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
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
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
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
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
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(false), Option(true))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow readers to have compute access but should allow writers for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)
    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                              samDAO = samDAO,
                                              fastPassServiceConstructor = (_, _) => mockFastPassService
    )(defaultRequestContext)

    val writerAclUpdate = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(false), Option(true))
    )
    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), writerAclUpdate, true), Duration.Inf)

    val readerAclUpdate = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(false), Option(true))
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), readerAclUpdate, true), Duration.Inf)
    }
    thrown.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "allow readers with and without share access for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)
    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                              samDAO = samDAO,
                                              fastPassServiceConstructor = (_, _) => mockFastPassService
    )(defaultRequestContext)

    val aclUpdate = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(true), Option(false)),
      WorkspaceACLUpdate("foo@bar.com", WorkspaceAccessLevels.Read, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdate, true), Duration.Inf)
  }

  "getWorkspaceSettings" should "return the workspace settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val workspaceSettings = List(
      WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                       WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(workspaceSettings))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)(defaultRequestContext)

    val returnedSettings = Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    returnedSettings shouldEqual workspaceSettings
    verify(samDAO).userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 ArgumentMatchers.eq(workspaceId.toString),
                                 ArgumentMatchers.eq(SamWorkspaceActions.read),
                                 any()
    )
  }

  it should "handle a workspace with no settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List.empty))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)(defaultRequestContext)

    val returnedSettings = Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    returnedSettings shouldEqual List.empty
    verify(samDAO).userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                 ArgumentMatchers.eq(workspaceId.toString),
                                 ArgumentMatchers.eq(SamWorkspaceActions.read),
                                 any()
    )
  }

  it should "return an error if the user does not have read access to the workspace" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(false))
    val service =
      workspaceServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)(defaultRequestContext)

    assertThrows[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceSettings(workspaceName), Duration.Inf)
    }
  }

  "setWorkspaceSettings" should "set the workspace settings if there aren't any set" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val workspaceSetting = WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                                            WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List.empty))
    when(workspaceRepository.createWorkspaceSettingsRecords(workspaceId, List(workspaceSetting)))
      .thenReturn(Future.successful(List(workspaceSetting)))
    when(workspaceRepository.markWorkspaceSettingApplied(workspaceId, workspaceSetting.`type`))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List())).thenReturn(Future.successful())

    val service = workspaceServiceConstructor(samDAO = samDAO,
                                              workspaceRepository = workspaceRepository,
                                              gcsDAO = gcsDAO
    )(defaultRequestContext)

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(workspaceSetting)), Duration.Inf)
    res.successes should contain theSameElementsAs List(workspaceSetting)
    res.failures shouldEqual Map.empty
  }

  it should "overwrite existing settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Set("prefixToMatch"), Some(30))
          )
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Set("muchBetterPrefix"), Some(31))
          )
        )
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List(existingSetting)))
    when(workspaceRepository.createWorkspaceSettingsRecords(workspaceId, List(newSetting)))
      .thenReturn(Future.successful(List(newSetting)))
    when(workspaceRepository.markWorkspaceSettingApplied(workspaceId, newSetting.`type`))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    val newSettingGoogleRule = new LifecycleRule(
      LifecycleAction.newDeleteAction(),
      LifecycleCondition.newBuilder().setMatchesPrefix(List("muchBetterPrefix").asJava).setAge(31).build()
    )
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List(newSettingGoogleRule))).thenReturn(Future.successful())

    val service = workspaceServiceConstructor(samDAO = samDAO,
                                              workspaceRepository = workspaceRepository,
                                              gcsDAO = gcsDAO
    )(defaultRequestContext)

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    res.successes should contain theSameElementsAs List(newSetting)
    res.failures shouldEqual Map.empty
  }

  it should "remove existing settings if no settings are specified" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Set("prefixToMatch"), Some(30))
          )
        )
      )
    )
    val defaultSetting = WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                                          WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List(existingSetting)))
    when(workspaceRepository.createWorkspaceSettingsRecords(workspaceId, List.empty))
      .thenReturn(Future.successful(List.empty))
    when(workspaceRepository.markWorkspaceSettingApplied(workspaceId, defaultSetting.`type`))
      .thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List())).thenReturn(Future.successful())

    val service = workspaceServiceConstructor(samDAO = samDAO,
                                              workspaceRepository = workspaceRepository,
                                              gcsDAO = gcsDAO
    )(defaultRequestContext)

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List.empty), Duration.Inf)
    res.successes should contain theSameElementsAs List(defaultSetting)
    res.failures shouldEqual Map.empty
  }

  it should "report errors while applying settings and remove pending settings" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Set("prefixToMatch"), Some(30))
          )
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Set("muchBetterPrefix"), Some(31))
          )
        )
      )
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))
    when(workspaceRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List(existingSetting)))
    when(workspaceRepository.createWorkspaceSettingsRecords(workspaceId, List(newSetting)))
      .thenReturn(Future.successful(List(newSetting)))
    when(workspaceRepository.removePendingSetting(workspaceId, newSetting.`type`)).thenReturn(Future.successful(1))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    val newSettingGoogleRule = new LifecycleRule(
      LifecycleAction.newDeleteAction(),
      LifecycleCondition.newBuilder().setMatchesPrefix(List("muchBetterPrefix").asJava).setAge(31).build()
    )
    when(gcsDAO.setBucketLifecycle(workspace.bucketName, List(newSettingGoogleRule)))
      .thenReturn(Future.failed(new Exception("failed to apply settings")))

    val service = workspaceServiceConstructor(samDAO = samDAO,
                                              workspaceRepository = workspaceRepository,
                                              gcsDAO = gcsDAO
    )(defaultRequestContext)

    val res = Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    res.successes shouldEqual List.empty
    res.failures(WorkspaceSettingTypes.GcpBucketLifecycle) shouldEqual ErrorReport(StatusCodes.InternalServerError,
                                                                                   "failed to apply settings"
    )
    verify(workspaceRepository).removePendingSetting(workspaceId, newSetting.`type`)
  }

  it should "be limited to owners" in {
    val workspaceId = workspace.workspaceIdAsUUID
    val workspaceName = workspace.toWorkspaceName
    val newSetting = WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle,
                                      WorkspaceSettingTypes.GcpBucketLifecycle.defaultConfig()
    )

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future.successful(Option(workspace)))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.own),
                           any()
      )
    ).thenReturn(Future.successful(false))
    // Rawls confirms a user has at least read access to the workspace after a failed authz check
    // to determine if it should throw a 403 or 404
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                           ArgumentMatchers.eq(workspaceId.toString),
                           ArgumentMatchers.eq(SamWorkspaceActions.read),
                           any()
      )
    ).thenReturn(Future.successful(true))

    val service =
      workspaceServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)(defaultRequestContext)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "validate requested settings" in {
    val workspaceName = workspace.toWorkspaceName
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("SetStorageClass"),
                                 GcpBucketLifecycleCondition(Set("prefixToMatch"), Some(-1))
          )
        )
      )
    )

    val service = workspaceServiceConstructor()(defaultRequestContext)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.setWorkspaceSettings(workspaceName, List(newSetting)), Duration.Inf)
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    exception.errorReport.message should include("Invalid settings requested.")
    exception.errorReport.causes should contain theSameElementsAs List(
      ErrorReport("Invalid GcpBucketLifecycle configuration: age must be a non-negative integer."),
      ErrorReport("Invalid GcpBucketLifecycle configuration: unsupported lifecycle action SetStorageClass.")
    )
  }
}
