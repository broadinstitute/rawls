package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{CloneWorkspaceResult, JobReport}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceName,
  WorkspaceRequest,
  WorkspaceState
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceCloneSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues {

  implicit val executionContext: TestExecutionContext = new TestExecutionContext()
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")

  val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  val namespace: String = "fake-namespace"
  val name: String = "fake-name"
  val workspaceName: WorkspaceName = WorkspaceName(namespace, name)
  val workspaceId: UUID = UUID.randomUUID()
  val defaultWorkspace: Workspace = Workspace.buildMcWorkspace(
    namespace = namespace,
    name = name,
    workspaceId = workspaceId.toString,
    DateTime.now(),
    DateTime.now(),
    createdBy = testContext.userInfo.userEmail.value,
    attributes = Map.empty,
    state = WorkspaceState.Ready
  )

  behavior of "cloneMultiCloudWorkspace"

  it should "fail if the user does not have access to the source workspace" in {
    val sourceWorkspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(false))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = intercept[NoSuchWorkspaceException] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
        ),
        Duration.Inf
      )
    }

    result.workspace shouldBe (sourceWorkspace.toWorkspaceName.toString)
  }

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val sourceWorkspace = defaultWorkspace
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))

    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))

    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(false))

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  /*
  TODO: test switch case:
   billingProfileOpt <- getBillingProfile(billingProject)
      (clone, cloudPlatform) <- (sourceWs.workspaceType, billingProfileOpt) match {

        case (McWorkspace, Some(profile)) if profile.getCloudPlatform == CloudPlatform.AZURE =>
          traceFutureWithParent("cloneAzureWorkspace", ctx) { s =>
            cloneAzureWorkspace(sourceWs, profile, destWorkspaceRequest, s).map(workspace =>
              (workspace, WorkspaceCloudPlatform.Azure)
            )
          }

        case (RawlsWorkspace, profileOpt)
            if profileOpt.isEmpty ||
              profileOpt.map(_.getCloudPlatform).contains(CloudPlatform.GCP) =>
          traceFutureWithParent("cloneRawlsWorkspace", ctx) { s =>
            wsService
              .cloneWorkspace(sourceWs, billingProject, destWorkspaceRequest, s)
              .map(workspace => (workspace, WorkspaceCloudPlatform.Gcp))
          }

        case (wsType, profileOpt) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(
                StatusCodes.BadRequest,
                s"Cloud platform mismatch: Cannot clone $wsType workspace '$sourceWorkspaceName' " +
                  s"into billing project '${billingProject.projectName}' " +
                  s"(hosted on ${profileOpt.map(_.getCloudPlatform).getOrElse("Unknown")})."
              )
            )
          )
      }
   */

  behavior of "cloneAzureWorkspace"

  it should "throw an exception if the billing profile was created before 9/12/2023" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneAzureWorkspace(
          defaultWorkspace,
          new ProfileModel().createdDate("2023-09-11T22:20:48.949Z"),
          WorkspaceRequest("dest-namespace", "dest-name", Map.empty),
          testContext
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode.value shouldBe StatusCodes.Forbidden
  }

  it should "fail if the destination workspace already exists" in {
    // This is covered in the tests for the workspace repository
  }

  it should "clean up the dest workspace record if the request to Workspace Manager fails" in {
    val workspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .tenantId(UUID.randomUUID())
      .subscriptionId(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .managedResourceGroupId("fake-mrg")
      .createdDate("2023-09-12T22:20:48.949Z")
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(workspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(destWorkspace))
    when(workspaceRepository.deleteWorkspace(destWorkspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceWorkspace = defaultWorkspace
    when(
      wsmDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        any(),
        ArgumentMatchers.eq(workspaceRequest.name),
        any(),
        ArgumentMatchers.eq(workspaceRequest.namespace),
        ArgumentMatchers.eq(testContext),
        any()
      )
    ).thenAnswer(_ => throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ImATeapot, "short and stout")))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspace(destWorkspace)
  }

  it should "clean up the dest workspace record if the Workspace Manager job fails asynchronously" in {
    val workspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .tenantId(UUID.randomUUID())
      .subscriptionId(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .managedResourceGroupId("fake-mrg")
      .createdDate("2023-09-12T22:20:48.949Z")
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(workspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(destWorkspace))
    when(workspaceRepository.deleteWorkspace(destWorkspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceWorkspace = defaultWorkspace
    val jobId = UUID.randomUUID().toString
    val cloneJob = new CloneWorkspaceResult().jobReport(new JobReport().id(jobId).status(StatusEnum.FAILED))
    when(
      wsmDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        any(),
        ArgumentMatchers.eq(workspaceRequest.name),
        any(),
        ArgumentMatchers.eq(workspaceRequest.namespace),
        ArgumentMatchers.eq(testContext),
        any()
      )
    ).thenReturn(cloneJob)
    when(wsmDAO.getCloneWorkspaceResult(any(), ArgumentMatchers.eq(jobId), ArgumentMatchers.eq(testContext)))
      .thenReturn(cloneJob)

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[WorkspaceManagerOperationFailureException] {
      Await.result(
        service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspace(destWorkspace)
    verify(wsmDAO).getCloneWorkspaceResult(any(), ArgumentMatchers.eq(jobId), ArgumentMatchers.eq(testContext))
    verify(wsmDAO).cloneWorkspace(
      ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
      any(),
      ArgumentMatchers.eq(workspaceRequest.name),
      any(),
      ArgumentMatchers.eq(workspaceRequest.namespace),
      ArgumentMatchers.eq(testContext),
      any()
    )
  }

  behavior of "cloneWorkspaceStorageContainer"

}
