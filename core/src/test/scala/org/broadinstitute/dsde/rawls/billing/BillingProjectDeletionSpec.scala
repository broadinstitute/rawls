import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.client.{ApiException => BpmApiException}
import bio.terra.profile.model.ProfileModel
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingProjectDeletion, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  UserInfo
}
import org.mockito.Mockito.{doNothing, verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProjectDeletionSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  val billingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("fake_name")
  val createRequest: CreateRawlsV2BillingProjectFullRequest = CreateRawlsV2BillingProjectFullRequest(
    billingProjectName,
    Some(RawlsBillingAccountName("fake_billing_account_name")),
    None,
    None,
    None,
    None
  )
  val profileModel: ProfileModel = new ProfileModel().id(UUID.randomUUID())

  behavior of "finalizeDelete"

  def mockBillingRepository(): BillingRepository = {
    val billingProfileId = profileModel.getId
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(repo.deleteBillingProject(ArgumentMatchers.any())).thenReturn(Future.successful(true))
    when(repo.getBillingProjectsWithProfile(Some(billingProfileId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(
            billingProjectName,
            CreationStatuses.Ready,
            None,
            None,
            billingProfileId = Some(billingProfileId.toString)
          )
        )
      )
    )
    repo
  }

  it should "delete the billing profile if other no projects reference it" in {
    val repo = mockBillingRepository()
    val bpm = mock[BillingProfileManagerDAO]
    doNothing()
      .when(bpm)
      .deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext))

    val billingProjectDeletion = new BillingProjectDeletion(mock[SamDAO], repo, bpm)
    Await.result(billingProjectDeletion.finalizeDelete(billingProjectName, testContext), Duration.Inf)

    verify(bpm).deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext))
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "not delete the billing profile if other projects reference it" in {
    val repo = mockBillingRepository()
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(billingProjectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          ),
          RawlsBillingProject(RawlsBillingProjectName("other_billing_project"),
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )

    val bpm = mock[BillingProfileManagerDAO]
    val billingProjectDeletion = new BillingProjectDeletion(mock[SamDAO], repo, bpm)
    Await.result(billingProjectDeletion.finalizeDelete(billingProjectName, testContext), Duration.Inf)

    verify(bpm, Mockito.never).deleteBillingProfile(profileModel.getId, testContext)
    // Billing project is still deleted
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "succeed if the billing profile id does not exist" in {
    val repo = mock[BillingRepository]
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.deleteBillingProject(ArgumentMatchers.eq(billingProjectName))).thenReturn(Future.successful(true))

    val bpm = mock[BillingProfileManagerDAO]
    val billingProjectDeletion = new BillingProjectDeletion(mock[SamDAO], repo, bpm)
    Await.result(billingProjectDeletion.finalizeDelete(billingProjectName, testContext), Duration.Inf)

    verify(bpm, Mockito.never()).deleteBillingProfile(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "fail on non-404 errors from BPM" in {
    val repo = mockBillingRepository()
    val bpm = mock[BillingProfileManagerDAO]

    when(bpm.deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext)))
      .thenAnswer(_ => throw new BpmApiException(HttpStatus.SC_FORBIDDEN, "forbidden"))

    val billingProjectDeletion = new BillingProjectDeletion(mock[SamDAO], repo, bpm)

    intercept[BpmApiException] {
      Await.result(billingProjectDeletion.finalizeDelete(billingProjectName, testContext), Duration.Inf)
    }
    verify(bpm).deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext))
    verify(repo, Mockito.never).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

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

    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val billingProjectDeletion = new BillingProjectDeletion(samDAOMock, repo, bpmDAO)

    intercept[Throwable] {
      Await.result(billingProjectDeletion.unregisterBillingProject(billingProjectName, testContext), Duration.Inf)
    }

    verify(repo).deleteBillingProject(billingProjectName)
  }
}
