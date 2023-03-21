package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  RawlsBillingAccountName,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectPolicyNames,
  SamResourceTypeNames,
  SamServicePerimeterActions,
  ServicePerimeterName,
  UserInfo
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.Mockito.{verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleBillingProjectLifecycleSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when creating a billing project against an billing account with no access" in {
    val samDAO = mock[SamDAO]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val gcsDAO = mock[GoogleServicesDAO]
    when(
      gcsDAO.testBillingAccountAccess(ArgumentMatchers.eq(createRequest.billingAccount.get),
                                      ArgumentMatchers.eq(userInfo)
      )
    ).thenReturn(Future.successful(false))
    val gbp = new GoogleBillingProjectLifecycle(mock[BillingRepository],samDAO, gcsDAO)

    val ex = intercept[GoogleBillingAccountAccessException] {
      Await.result(gbp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when provided with a service perimeter name but no access" in {
    val servicePerimeterName = ServicePerimeterName("fake_sp_name")
    val samDAO = mock[SamDAO]

    when(
      samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
        ArgumentMatchers.eq(servicePerimeterName.value),
        ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
        ArgumentMatchers.eq(testContext)
      )
    ).thenReturn(Future.successful(false))

    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_billing_project"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      Some(servicePerimeterName),
      None,
      None,
      None
    )
    val bpo = new GoogleBillingProjectLifecycle(
      mock[BillingRepository],
      samDAO,
      mock[GoogleServicesDAO]
    )

    val ex = intercept[ServicePerimeterAccessException] {
      Await.result(bpo.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Forbidden)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "postCreationSteps"

  it should "sync the policy to google and return creation status Ready" in {
    val samDAO = mock[SamDAO]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val gbp = new GoogleBillingProjectLifecycle(mock[BillingRepository],samDAO, mock[GoogleServicesDAO])

    assertResult(CreationStatuses.Ready) {
      Await.result(gbp.postCreationSteps(createRequest, mock[MultiCloudWorkspaceConfig], testContext), Duration.Inf)
    }

    verify(samDAO, Mockito.times(1))
      .syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
  }
}
