package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.client.{ApiException => BpmApiException}
import bio.terra.profile.model.ProfileModel
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ProjectAccessUpdate,
  ProjectRoles,
  RawlsBillingAccountName,
  RawlsBillingProject,
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
import org.mockito.Mockito.{doNothing, doReturn, verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class GoogleBillingProjectLifecycleSpec extends AnyFlatSpec {
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
      gcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(createRequest.billingAccount.get),
                                                  ArgumentMatchers.eq(userInfo)
      )
    ).thenReturn(Future.successful(false))
    val gbp = new GoogleBillingProjectLifecycle(mock[BillingRepository], samDAO, gcsDAO)

    val ex = intercept[GoogleBillingAccountAccessException] {
      Await.result(
        gbp.validateBillingProjectCreationRequest(createRequest, mock[BillingProfileManagerDAO], testContext),
        Duration.Inf
      )
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
      Await.result(
        bpo.validateBillingProjectCreationRequest(createRequest, mock[BillingProfileManagerDAO], testContext),
        Duration.Inf
      )
    }

    assertResult(Some(StatusCodes.Forbidden)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "postCreationSteps"

  it should "sync the policy to google and return creation status Ready" in {
    val repo = mock[BillingRepository]
    val samDAO = mock[SamDAO]
    val bpm = mock[BillingProfileManagerDAO]
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
        SamResourceTypeNames.billingProject,
        createRequest.projectName.value,
        SamBillingProjectPolicyNames.owner
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val gbp = new GoogleBillingProjectLifecycle(repo, samDAO, mock[GoogleServicesDAO])

    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(createRequest.billingInfo),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)

    assertResult(CreationStatuses.Ready) {
      Await.result(gbp.postCreationSteps(createRequest, mock[MultiCloudWorkspaceConfig], bpm, testContext),
                   Duration.Inf
      )
    }

    verify(samDAO)
      .syncPolicyToGoogle(
        SamResourceTypeNames.billingProject,
        createRequest.projectName.value,
        SamBillingProjectPolicyNames.owner
      )
  }

  it should "store the billing profile ID during billing project creation" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val samDAO = mock[SamDAO]
    val wsmResourceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    val bp = new GoogleBillingProjectLifecycle(repo, samDAO, mock[GoogleServicesDAO])

    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(createRequest.billingInfo),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)

    when(
      samDAO.syncPolicyToGoogle(
        SamResourceTypeNames.billingProject,
        createRequest.projectName.value,
        SamBillingProjectPolicyNames.owner
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))

    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    doReturn(Future.successful())
      .when(wsmResourceRecordDao)
      .create(ArgumentMatchers.any)

    Await.result(bp.postCreationSteps(
                   createRequest,
                   mock[MultiCloudWorkspaceConfig],
                   bpm,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(repo).setBillingProfileId(
      createRequest.projectName,
      profileModel.getId
    )
  }

  it should "add additional members to the BPM policy during billing project creation if specified" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val samDAO = mock[SamDAO]
    val wsmResourceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    val bp = new GoogleBillingProjectLifecycle(repo, samDAO, mock[GoogleServicesDAO])

    val user1Email = "user1@foo.bar"
    val user2Email = "user2@foo.bar"
    val user3Email = "user3@foo.bar"

    val createRequestWithMembers = createRequest.copy(members =
      Some(
        Set(
          ProjectAccessUpdate(user1Email, ProjectRoles.Owner),
          ProjectAccessUpdate(user2Email, ProjectRoles.Owner),
          ProjectAccessUpdate(user3Email, ProjectRoles.User)
        )
      )
    )

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequestWithMembers.projectName.value),
        ArgumentMatchers.eq(createRequestWithMembers.billingInfo),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)

    when(
      samDAO.syncPolicyToGoogle(
        SamResourceTypeNames.billingProject,
        createRequest.projectName.value,
        SamBillingProjectPolicyNames.owner
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))

    when(repo.setBillingProfileId(createRequestWithMembers.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    doReturn(Future.successful())
      .when(wsmResourceRecordDao)
      .create(ArgumentMatchers.any)

    Await.result(bp.postCreationSteps(
                   createRequestWithMembers,
                   mock[MultiCloudWorkspaceConfig],
                   bpm,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.Owner),
      ArgumentMatchers.eq(user1Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.Owner),
      ArgumentMatchers.eq(user2Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.User),
      ArgumentMatchers.eq(user3Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
  }

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

    val bp = new GoogleBillingProjectLifecycle(repo, mock[SamDAO], mock[GoogleServicesDAO])
    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

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
    val bp = new GoogleBillingProjectLifecycle(repo, mock[SamDAO], mock[GoogleServicesDAO])
    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

    verify(bpm, Mockito.never).deleteBillingProfile(profileModel.getId, testContext)
    // Billing project is still deleted
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "succeed if the billing profile id does not exist" in {
    val repo = mock[BillingRepository]
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.deleteBillingProject(ArgumentMatchers.eq(billingProjectName))).thenReturn(Future.successful(true))

    val bpm = mock[BillingProfileManagerDAO]
    val bp = new GoogleBillingProjectLifecycle(repo, mock[SamDAO], mock[GoogleServicesDAO])
    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

    verify(bpm, Mockito.never()).deleteBillingProfile(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "fail on non-404 errors from BPM" in {
    val repo = mockBillingRepository()
    val bpm = mock[BillingProfileManagerDAO]

    when(bpm.deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext)))
      .thenAnswer(_ => throw new BpmApiException(HttpStatus.SC_FORBIDDEN, "forbidden"))

    val bp = new GoogleBillingProjectLifecycle(repo, mock[SamDAO], mock[GoogleServicesDAO])

    intercept[BpmApiException] {
      Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)
    }
    verify(bpm).deleteBillingProfile(ArgumentMatchers.eq(profileModel.getId), ArgumentMatchers.eq(testContext))
    verify(repo, Mockito.never).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }
}
