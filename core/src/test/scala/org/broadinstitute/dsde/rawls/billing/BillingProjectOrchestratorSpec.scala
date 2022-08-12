package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, RawlsUserEmail, RawlsUserSubjectId, SamBillingProjectPolicyNames, SamCreateResourceResponse, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProjectOrchestratorSpec extends AnyFlatSpec {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo = UserInfo(
    RawlsUserEmail("fake@example.com"),
    OAuth2BearerToken("fake_token"),
    0,
    RawlsUserSubjectId("sub"),
    None)

  behavior of "creation request validation"

  it should "fail when provided an invalid billing project name" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    val billingRepository = mock[BillingRepository]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("!@B#$"),
      RawlsBillingAccountName("fake_billing_account_name"),
      None
    )
    val bpo = new BillingProjectOrchestrator(
      userInfo,
      samDAO,
      gcsDAO,
      billingRepository
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when creating a billing project against an billing account with no access" in {
    val samDAO = mock[SamDAO]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      RawlsBillingAccountName("fake_billing_account_name"),
      None
    )
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(
      ArgumentMatchers.eq(createRequest.billingAccount),
      ArgumentMatchers.eq(userInfo))
    ).thenReturn(Future.successful(false))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(billingRepository.createBillingProject(any[RawlsBillingProject]))
      .thenReturn(Future.successful(RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value), CreationStatuses.Ready, None, None)))
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo)),
        ArgumentMatchers.eq(Set.empty),
        ArgumentMatchers.eq(userInfo),
        ArgumentMatchers.eq(None)
      )).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner))
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val bpo = new BillingProjectOrchestrator(
      userInfo,
      samDAO,
      gcsDAO,
      billingRepository
    )

    val ex = intercept[GoogleBillingAccountAccessException] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when provided with a service perimeter name but no access" in {
    val servicePerimeterName = ServicePerimeterName("fake_sp_name")
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]

    when(
      samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
        ArgumentMatchers.eq(servicePerimeterName.value),
        ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
        ArgumentMatchers.eq(userInfo))).thenReturn(Future.successful(false))

    val gcsDAO = mock[GoogleServicesDAO]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_billing_project"),
      RawlsBillingAccountName("fake_billing_account_name"),
      Some(servicePerimeterName)
    )
    val bpo = new BillingProjectOrchestrator(
      userInfo,
      samDAO,
      gcsDAO,
      billingRepository
    )

    val ex = intercept[ServicePerimeterAccessException] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Forbidden)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "billing project creation"

  it should "create a billing project record when provided a valid request" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo))).thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      RawlsBillingAccountName("fake_billing_account_name"),
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(billingRepository.createBillingProject(
      any[RawlsBillingProject])
    ).thenReturn(
      Future.successful(RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value), CreationStatuses.Ready, None, None))
    )
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo)),
        ArgumentMatchers.eq(Set.empty),
        any[UserInfo],
        ArgumentMatchers.eq(None)
      )).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner))
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))

    val bpo = new BillingProjectOrchestrator(
      userInfo,
      samDAO,
      gcsDAO,
      billingRepository
    )

    Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
  }

  it should "fail when a duplicate project already exists" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo))).thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      RawlsBillingAccountName("fake_billing_account_name"),
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName))).thenReturn(
      Future.successful(Some(RawlsBillingProject(RawlsBillingProjectName("fake"), CreationStatuses.Ready, None, None)))
    )
    val bpo = new BillingProjectOrchestrator(
      userInfo,
      samDAO,
      gcsDAO,
      billingRepository
    )

    val ex = intercept[DuplicateBillingProjectException] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Conflict)) {
      ex.errorReport.statusCode
    }
  }

}
