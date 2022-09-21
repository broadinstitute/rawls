package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.model.{IamRole, RoleBinding, RoleBindingList}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, UserDisabledException, WorkspaceAccessDeniedException}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
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

/**
  * Unit tests kept separate from WorkspaceServiceSpec to separate true unit tests from tests requiring external resources
  */
class WorkspaceServiceUnitTests extends AnyFlatSpec with OptionValues with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  def workspaceServiceConstructor(
    datasource: SlickDataSource = mock[SlickDataSource],
    methodRepoDAO: MethodRepoDAO = mock[MethodRepoDAO],
    cromiamDAO: ExecutionServiceDAO = mock[ExecutionServiceDAO],
    executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster],
    execServiceBatchSize: Int = 1,
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO],
    methodConfigResolver: MethodConfigResolver = mock[MethodConfigResolver],
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO],
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO],
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService],
    genomicsServiceConstructor: RawlsRequestContext => GenomicsService = _ => mock[GenomicsService],
    maxActiveWorkflowsTotal: Int = 1,
    maxActiveWorkflowsPerUser: Int = 1,
    workbenchMetricBaseName: String = "",
    submissionCostService: SubmissionCostService = mock[SubmissionCostService],
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig],
    requesterPaysSetupService: RequesterPaysSetupService = mock[RequesterPaysSetupService],
    entityManager: EntityManager = mock[EntityManager],
    resourceBufferService: ResourceBufferService = mock[ResourceBufferService],
    resourceBufferSaEmail: String = "",
    servicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService],
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO],
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = ""
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
      terraWorkspaceNextflowRole
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

  "getWorkspaceById" should "return the exception thrown by getWorkspace(WorkspaceName) on failure" in {
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

  "getWorkspaceById" should "return an exception without the workspace name when getWorkspace(WorkspaceName) is not found" in {
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

  "getWorkspaceById" should "return an exception without the workspace name when getWorkspace(WorkspaceName) fails access checks" in {
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

  "getWorkspaceById" should "return an exception with the workspaceId when no workspace is found in the initial query" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List()))

    val workspaceId = "c1e14bc7-cc7f-4710-a383-74370be3cba1"

    val exception = intercept[NoSuchWorkspaceException] {
      val service = workspaceServiceConstructor(datasource)(defaultRequestContext)
      Await.result(service.getWorkspaceById(workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    assert(exception.workspace == workspaceId)
  }

  "getWorkspace" should "return an unauthorized error if the user is disabled" in {
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(Future.successful(List()))
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val samUserStatus = SamUserStatusResponse("sub", "email", enabled = false)
    when(samDAO.getUserStatus(ArgumentMatchers.eq(defaultRequestContext.userInfo))).thenReturn(
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

  "getAcl" should "fetch policies from Sam for Rawls workspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val samPolicies = Set(
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
      )
    )

    val datasource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(datasource.inTransaction[Workspace](any(), any())).thenReturn(
      Future.successful(
        Workspace("fake_ns",
                  "fake_name",
                  UUID.randomUUID().toString,
                  "fake_bucket",
                  None,
                  DateTime.now(),
                  DateTime.now(),
                  "creator@example.com",
                  Map.empty
        ).copy(workspaceType = WorkspaceType.RawlsWorkspace)
      )
    )
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserIdInfo(any(), any())).thenReturn(
      Future.successful(SamDAO.User(UserIdInfo("fake_user_id", "user@example.com", Option("fake_google_subject_id"))))
    )
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samPolicies)
    )

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
    val ownerBinding = new RoleBinding().role(IamRole.OWNER).members(List(ownerEmail).asJava)
    val writerBinding = new RoleBinding().role(IamRole.WRITER).members(List(writerEmail).asJava)
    val readerBinding = new RoleBinding().role(IamRole.READER).members(List(readerEmail).asJava)
    val wsmRoleBindings = new RoleBindingList()
    wsmRoleBindings.addAll(List(ownerBinding, writerBinding, readerBinding).asJava)

    val datasource = mock[SlickDataSource](RETURNS_SMART_NULLS)
    when(datasource.inTransaction[Workspace](any(), any())).thenReturn(
      Future.successful(
        Workspace("fake_ns",
                  "fake_name",
                  UUID.randomUUID().toString,
                  "fake_bucket",
                  None,
                  DateTime.now(),
                  DateTime.now(),
                  "creator@example.com",
                  Map.empty
        ).copy(workspaceType = WorkspaceType.McWorkspace)
      )
    )

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserIdInfo(any(), any())).thenReturn(
      Future.successful(SamDAO.User(UserIdInfo("fake_user_id", "user@example.com", Option("fake_google_subject_id"))))
    )
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))

    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDAO.getRoles(any(), any())).thenReturn(wsmRoleBindings)

    val expected = WorkspaceACL(
      Map(
        ownerEmail -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
        writerEmail -> AccessEntry(WorkspaceAccessLevels.Write, false, false, false),
        readerEmail -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
      )
    )

    val service =
      workspaceServiceConstructor(datasource, samDAO = samDAO, workspaceManagerDAO = wsmDAO)(defaultRequestContext)
    val result = Await.result(service.getACL(WorkspaceName("fake_namespace", "fake_name")), Duration.Inf)

    result shouldBe expected
    verify(samDAO, never).listPoliciesForResource(any(), any(), any())
    verify(wsmDAO).getRoles(any(), any())
  }
}
