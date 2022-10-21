package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectPolicyNames,
  SamCreateResourceResponse,
  SamResourceTypeNames,
  UserInfo
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProjectOrchestratorSpec extends AnyFlatSpec {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val azConfig: AzureConfig = AzureConfig(
    "fake-sp-id",
    UUID.randomUUID().toString,
    UUID.randomUUID().toString,
    "fake-mrg-id",
    "fake-bp-name",
    "fake-alpha-feature-group",
    "eastus",
    "fake-landing-zone-definition",
    "fake-landing-zone-version"
  )

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  behavior of "creation request validation"

  it should "fail when the billing project fails validation" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("!@B#$"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val gbp = mock[BillingProjectLifecycle]
    when(gbp.validateBillingProjectCreationRequest(createRequest, testContext))
      .thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "failed"))))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      billingRepository,
      gbp,
      mock[BpmBillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "billing project creation"

  it should "create a billing project record when provided a valid request and set the correct creation status" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val bpCreator = mock[BillingProjectLifecycle]
    val bpCreatorReturnedStatus = CreationStatuses.CreatingLandingZone
    val multiCloudWorkspaceConfig = new MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    when(bpCreator.validateBillingProjectCreationRequest(createRequest, testContext)).thenReturn(Future.successful())
    when(bpCreator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, testContext))
      .thenReturn(Future.successful(bpCreatorReturnedStatus))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(billingRepository.createBillingProject(any[RawlsBillingProject])).thenReturn(
      Future.successful(
        RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value),
                            CreationStatuses.Creating,
                            None,
                            None
        )
      )
    )
    when(
      billingRepository.updateCreationStatus(ArgumentMatchers.eq(createRequest.projectName),
                                             ArgumentMatchers.eq(bpCreatorReturnedStatus),
                                             any()
      )
    ).thenReturn(Future.successful(1))
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.defaultBillingProjectPolicies(testContext)),
        ArgumentMatchers.eq(Set.empty),
        any[RawlsRequestContext],
        ArgumentMatchers.eq(None)
      )
    ).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      multiCloudWorkspaceConfig
    )

    Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)

    verify(billingRepository, Mockito.times(1)).updateCreationStatus(ArgumentMatchers.eq(createRequest.projectName),
                                                                     ArgumentMatchers.eq(bpCreatorReturnedStatus),
                                                                     ArgumentMatchers.eq(None)
    )
  }

  it should "fail when a duplicate project already exists" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName))).thenReturn(
      Future.successful(Some(RawlsBillingProject(RawlsBillingProjectName("fake"), CreationStatuses.Ready, None, None)))
    )
    val bpCreator = mock[BillingProjectLifecycle]
    when(bpCreator.validateBillingProjectCreationRequest(createRequest, testContext)).thenReturn(Future.successful())

    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig]
    )

    val ex = intercept[DuplicateBillingProjectException] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Conflict)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when provided an invalid billing project name" in {
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("!@B#$"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val bpo = new BillingProjectOrchestrator(
      testContext,
      mock[SamDAO],
      mock[BillingRepository],
      mock[BillingProjectLifecycle],
      mock[BillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "delete the billing project and throw an exception if post creation steps fail" in {
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val creator = mock[BillingProjectLifecycle](RETURNS_SMART_NULLS)
    val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    when(
      creator.validateBillingProjectCreationRequest(ArgumentMatchers.eq(createRequest),
                                                    ArgumentMatchers.eq(testContext)
      )
    ).thenReturn(Future.successful())
    when(creator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, testContext))
      .thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, "Failed"))))
    val repo = mock[BillingRepository](RETURNS_SMART_NULLS)
    when(repo.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(repo.createBillingProject(any[RawlsBillingProject])).thenReturn(
      Future.successful(
        RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value),
                            CreationStatuses.Ready,
                            None,
                            None
        )
      )
    )
    when(repo.deleteBillingProject(ArgumentMatchers.eq(createRequest.projectName))).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.defaultBillingProjectPolicies(testContext)),
        ArgumentMatchers.eq(Set.empty),
        any[RawlsRequestContext],
        ArgumentMatchers.eq(None)
      )
    ).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    when(
      samDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                            ArgumentMatchers.eq(createRequest.projectName.value),
                            ArgumentMatchers.eq(testContext)
      )
    ).thenReturn(Future.successful())

    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      repo,
      creator,
      mock[BillingProjectLifecycle],
      multiCloudWorkspaceConfig
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadGateway)) {
      ex.errorReport.statusCode
    }
    verify(repo, Mockito.times(1)).deleteBillingProject(ArgumentMatchers.eq(createRequest.projectName))
    verify(samDAO, Mockito.times(1)).deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                                    ArgumentMatchers.eq(createRequest.projectName.value),
                                                    ArgumentMatchers.eq(testContext)
    )
  }
}
