package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, SamBillingProjectActions, SamResourceTypeNames, UserInfo, WorkspaceName, WorkspaceRequest}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class MultiCloudWorkspaceServiceCreateSpec extends AnyFlatSpecLike
  with MockitoSugar
  with ScalaFutures
  with Matchers
  with OptionValues {

  implicit val executionContext: TestExecutionContext = new TestExecutionContext()
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")


  val userInfo = UserInfo(RawlsUserEmail("owner-access"),
    OAuth2BearerToken("token"),
    123,
    RawlsUserSubjectId("123456789876543212345")
  )
  val testContext = RawlsRequestContext(userInfo)

  val namespace = "fake-namespace"
  val name = "fake-name"
  val workspaceName = WorkspaceName(namespace, name)

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val billingProjectName = RawlsBillingProjectName("azure-billing-project")
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("azure-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )

    val samDAO = mock[SamDAO]
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProjectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(false))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
    )
    val workspaceRequest = WorkspaceRequest(
      billingProject.projectName.value,
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudOrRawlsWorkspace(workspaceRequest, mock[WorkspaceService]), Duration.Inf)
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

}

