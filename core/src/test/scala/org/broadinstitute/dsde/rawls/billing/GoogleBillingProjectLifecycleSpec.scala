package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
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
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, verify, when}
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
    val gbp = new GoogleBillingProjectLifecycle(mock[BillingRepository], mock[BillingProfileManagerDAO], samDAO, gcsDAO)

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
      mock[BillingProfileManagerDAO],
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
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val gbp = new GoogleBillingProjectLifecycle(mock[BillingRepository], bpm, samDAO, mock[GoogleServicesDAO])

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(createRequest.billingInfo),
        any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)

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

  it should "store the billing profile ID add additional members to the BPM policy during billing project creation" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val samDAO = mock[SamDAO]
    val wsmResourceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    val bp = new GoogleBillingProjectLifecycle(repo, bpm, samDAO, mock[GoogleServicesDAO])

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
        any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)

    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))

    when(repo.setBillingProfileId(createRequestWithMembers.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    doReturn(Future.successful())
      .when(wsmResourceRecordDao)
      .create(any)

    Await.result(bp.postCreationSteps(
                   createRequestWithMembers,
                   mock[MultiCloudWorkspaceConfig],
                   testContext
                 ),
                 Duration.Inf
    )

    verify(bpm, Mockito.times(2)).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.Owner),
      ArgumentMatchers.argThat(arg => Set(user1Email, user2Email).contains(arg)),
      any[RawlsRequestContext]
    )
    verify(bpm, Mockito.times(1)).addProfilePolicyMember(ArgumentMatchers.eq(profileModel.getId),
                                                         ArgumentMatchers.eq(ProfilePolicy.User),
                                                         ArgumentMatchers.eq(user3Email),
                                                         any[RawlsRequestContext]
    )
    verify(repo, Mockito.times(1)).setBillingProfileId(
      createRequestWithMembers.projectName,
      profileModel.getId
    )
  }
}
