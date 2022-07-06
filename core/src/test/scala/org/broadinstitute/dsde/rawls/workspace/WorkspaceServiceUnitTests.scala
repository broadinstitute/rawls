package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{UserInfo, _}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.OptionValues
import spray.json.{JsObject, JsString}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


/**
  * Unit tests kept separate from WorkspaceServiceSpec to separate true unit tests from tests requiring external resources
  */
class WorkspaceServiceUnitTests extends AnyFlatSpec with OptionValues with MockitoTestUtils {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

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
                                   userServiceConstructor: UserInfo => UserService = _ => mock[UserService],
                                   genomicsServiceConstructor: UserInfo => GenomicsService = _ => mock[GenomicsService],
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
                                   terraWorkspaceNextflowRole: String = ""): UserInfo => WorkspaceService = info =>
    WorkspaceService.constructor(
      datasource,
      methodRepoDAO, cromiamDAO, executionServiceCluster, execServiceBatchSize, workspaceManagerDAO, methodConfigResolver,
      gcsDAO, samDAO, notificationDAO, userServiceConstructor, genomicsServiceConstructor, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, workbenchMetricBaseName,
      submissionCostService, config, requesterPaysSetupService, entityManager, resourceBufferService, resourceBufferSaEmail, servicePerimeterService,
      googleIamDao, terraBillingProjectOwnerRole, terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)(info)(mock[Materializer], scala.concurrent.ExecutionContext.global) //, mock[ExecutionContext])


  "getWorkspaceById" should "return the workspace returned by getWorkspace(WorkspaceName) on success" in {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "cba")))
    val workspaceJs = new JsObject(Map("hi" -> JsString("ho")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = spy(workspaceServiceConstructor(datasource)(userInfo))
    doReturn(Future.successful(workspaceJs)).when(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
    val result = Await.result(service.getWorkspaceById(UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1").toString, WorkspaceFieldSpecs()), Duration.fromNanos(100000000))
    assertResult(workspaceJs)(result)
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
  }

  "getWorkspaceById" should "return the exception thrown by getWorkspace(WorkspaceName) on failure" in {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "cba")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = spy(workspaceServiceConstructor(datasource)(userInfo))
    val failure = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "A generic exception"))
    doReturn(Future.failed(failure)).when(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
    val result = Await.ready(service.getWorkspaceById(UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1").toString, WorkspaceFieldSpecs()), Duration.fromNanos(100000000))
    assertResult(failure)(result.failed.value.get.get)
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "cba")), any(), any())
  }

  "getWorkspaceById" should "return an exception without the workspace name when getWorkspace(WorkspaceName) is not found" in {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "123")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = spy(workspaceServiceConstructor(datasource)(userInfo))

    val failure = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, service.noSuchWorkspaceMessage(WorkspaceName("abc", "123"))))
    doReturn(Future.failed(failure)).when(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())
    val result = Await.ready(service.getWorkspaceById(UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1").toString, WorkspaceFieldSpecs()), Duration.fromNanos(100000000))
    assert(result.failed.value.get.get.getMessage.contains(service.noSuchWorkspaceMessage(UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1"))))
    assert(!result.failed.value.get.get.getMessage.contains("abc"))
    assert(!result.failed.value.get.get.getMessage.contains("123"))
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())
  }

  "getWorkspaceById" should "return an exception without the workspace name when getWorkspace(WorkspaceName) fails access checks" in {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List(("abc", "123")))
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = spy(workspaceServiceConstructor(datasource)(userInfo))
    val failure = new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, service.accessDeniedMessage(WorkspaceName("abc", "123"))))
    doReturn(Future.failed(failure)).when(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())

    val workspaceId = UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1")
    val result = Await.ready(service.getWorkspaceById(workspaceId.toString, WorkspaceFieldSpecs()), Duration.fromNanos(100000000))
    assert(result.failed.value.get.get.getMessage.contains(service.accessDeniedMessage(workspaceId)))
    assert(!result.failed.value.get.get.getMessage.contains("abc"))
    assert(!result.failed.value.get.get.getMessage.contains("123"))
    verify(service).getWorkspace(ArgumentMatchers.eq(WorkspaceName("abc", "123")), any(), any())
  }


  "getWorkspaceById" should "return an exception with the workspace is when no workspace is found in the initial query" in {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    val workspaceFields: Future[Seq[(String, String)]] = Future.successful(List()) //("abc", "123")
    val datasource = mock[SlickDataSource]
    when(datasource.inTransaction[Any](any(), any())).thenReturn(workspaceFields)
    val service = workspaceServiceConstructor(datasource)(userInfo)

    val workspaceId = UUID.fromString("c1e14bc7-cc7f-4710-a383-74370be3cba1")
    val result = Await.ready(
      workspaceServiceConstructor(datasource)(userInfo).getWorkspaceById(workspaceId.toString, WorkspaceFieldSpecs()), Duration.fromNanos(100000000)
    )
    assert(result.failed.value.get.get.getMessage.contains(service.noSuchWorkspaceMessage(workspaceId)))
    assert(!result.failed.value.get.get.getMessage.contains("abc"))
    assert(!result.failed.value.get.get.getMessage.contains("123"))
  }

}
