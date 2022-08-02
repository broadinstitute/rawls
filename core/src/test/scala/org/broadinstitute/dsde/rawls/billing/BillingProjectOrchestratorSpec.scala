package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreateRawlsV2BillingProjectFullRequest, CreationStatuses, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, RawlsUserEmail, RawlsUserSubjectId, SamBillingProjectPolicyNames, SamCreateResourceResponse, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
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
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when creating a billing project against an billing account with no access" in {
    val samDAO = mock[SamDAO]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(
      ArgumentMatchers.eq(createRequest.billingAccount.get),
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
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    val ex = intercept[GoogleBillingAccountAccessException] {
      Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
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
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      Some(servicePerimeterName),
      None
    )
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    val ex = intercept[ServicePerimeterAccessException] {
      Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
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
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
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
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
  }

  it should "fail when a duplicate project already exists" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo))).thenReturn(Future.successful(true))
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
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Conflict)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "azure billing project creation"

  it should "create a project" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    val billingRepository = mock[BillingRepository]
    val subId = UUID.randomUUID()
    val managedAppCoordinates = AzureManagedAppCoordinates("fake_tenant", subId.toString, "fake_mrg_id")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      None,
      None,
      Some(managedAppCoordinates)
    )
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo)),
        ArgumentMatchers.eq(Set.empty),
        ArgumentMatchers.eq(userInfo),
        ArgumentMatchers.eq(None)
      )).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName))).thenReturn(Future.successful(None))
    when(billingRepository.createBillingProject(
      any[RawlsBillingProject])
    ).thenReturn(
      Future.successful(RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value), CreationStatuses.Ready, None, None))
    )
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val profileId = UUID.randomUUID()
    when(billingProfileManagerDAO.createBillingProfile(
      ArgumentMatchers.eq(createRequest.projectName.value),
      ArgumentMatchers.eq(Right(managedAppCoordinates)), ArgumentMatchers.eq(userInfo))
    ).thenReturn(Future.successful(new ProfileModel().id(profileId)))
    when(billingRepository.setBillingProfileId(ArgumentMatchers.eq(createRequest.projectName), ArgumentMatchers.eq(profileId))).thenReturn(Future.successful(1))
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )

    Await.result(bpo.createBillingProjectV2(createRequest, userInfo), Duration.Inf)
  }

  it should "fail if neither managed app coordinates nor a gcp billing account is provided" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    val billingRepository = mock[BillingRepository]
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      None,
      None,
      None
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      bpo.createBillingProjectV2(createRequest, userInfo)
    }

    assertResult(Some(StatusCodes.BadRequest)){ex.errorReport.statusCode}
  }

  it should "fail if both managed app coordinates and a gcp billing account is provided" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    val billingRepository = mock[BillingRepository]
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      samDAO, gcsDAO, billingRepository, billingProfileManagerDAO
    )
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      Some(RawlsBillingAccountName("testing")),
      None,
      Some(AzureManagedAppCoordinates("testing", "testing", "testing"))
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      bpo.createBillingProjectV2(createRequest, userInfo)
    }

    assertResult(Some(StatusCodes.BadRequest)){ex.errorReport.statusCode}
  }

}
