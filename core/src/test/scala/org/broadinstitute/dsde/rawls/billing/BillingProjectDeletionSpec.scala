package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  RawlsBillingAccountName,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  UserInfo
}
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProjectDeletionSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)

  val billingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("fake_name")
  val createRequest: CreateRawlsV2BillingProjectFullRequest = CreateRawlsV2BillingProjectFullRequest(
    billingProjectName,
    Some(RawlsBillingAccountName("fake_billing_account_name")),
    None,
    None,
    None,
    None
  )

  behavior of "unregisterBillingProject"

  it should "delete the project even if Sam deleteResource fails" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val testContext = mock[RawlsRequestContext]

    val samDAOMock = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAOMock.userHasAction(SamResourceTypeNames.billingProject,
                               billingProjectName.value,
                               SamBillingProjectActions.deleteBillingProject,
                               testContext
      )
    ).thenReturn(Future.successful(true))

    when(samDAOMock.deleteResource(SamResourceTypeNames.billingProject, billingProjectName.value, testContext))
      .thenReturn(Future.failed(new Throwable("Sam failed")))

    val repo = mock[BillingRepository]
    when(repo.failUnlessHasNoWorkspaces(billingProjectName)(executionContext)).thenReturn(Future.successful())
    when(repo.deleteBillingProject(billingProjectName)).thenReturn(Future.successful(true))
    when(repo.getBillingProfileId(billingProjectName)(executionContext)).thenReturn(Future.successful(None))

    val billingProjectDeletion = new BillingProjectDeletion(samDAOMock, repo)

    intercept[Throwable] {
      Await.result(billingProjectDeletion.unregisterBillingProject(billingProjectName, testContext), Duration.Inf)
    }

    verify(repo).deleteBillingProject(billingProjectName)
  }
}
