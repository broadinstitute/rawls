package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamAdminDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  BillingProjectAdminResponse,
  CreationStatuses,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceTypeAdminActions,
  SamResourceTypeNames,
  UserInfo,
  Workspace,
  WorkspaceAdminResponse,
  WorkspaceDetails
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.{when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class BillingAdminServiceUnitTests extends AnyFlatSpec with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  def billingAdminServiceConstructor(
    samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS),
    billingRepository: BillingRepository = mock[BillingRepository](
      RETURNS_SMART_NULLS
    ),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    ctx: RawlsRequestContext = defaultRequestContext
  ): BillingAdminService =
    new BillingAdminService(
      samDAO,
      billingRepository,
      workspaceRepository,
      ctx
    )

  "getBillingProject" should "return the billing project with a list of its workspaces" in {
    val billingProject: RawlsBillingProject = RawlsBillingProject(RawlsBillingProjectName("project"),
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("account")),
                                                                  None
    )

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(billingProject.projectName))
      .thenReturn(Future.successful(Option(billingProject)))

    val workspace = Workspace(
      "billingAdminNamespace",
      "billingAdminWorkspace",
      UUID.randomUUID.toString,
      "bucketName",
      Some("workflowCollection"),
      new DateTime(),
      new DateTime(),
      "creator",
      Map.empty
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.listWorkspacesByBillingProject(billingProject.projectName))
      .thenReturn(Future.successful(Seq(workspace)))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service =
      billingAdminServiceConstructor(
        samDAO = samDAO,
        billingRepository = billingRepository,
        workspaceRepository = workspaceRepository
      )

    val returnedBillingProject = Await.result(service.getBillingProject(billingProject.projectName), Duration.Inf)
    returnedBillingProject shouldEqual BillingProjectAdminResponse(billingProject,
                                                                   Map(workspace.name -> workspace.workspaceIdAsUUID)
    )
  }

  it should "throw if the user is not an admin" in {
    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(false))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service = billingAdminServiceConstructor(samDAO = samDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getBillingProject(RawlsBillingProjectName("project")), Duration.Inf)
    }
    exception.errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
  }

  it should "throw if the billing project is not found" in {
    val projectName = RawlsBillingProjectName("project")
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(projectName)).thenReturn(Future.successful(None))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service =
      billingAdminServiceConstructor(
        samDAO = samDAO,
        billingRepository = billingRepository
      )

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getBillingProject(projectName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldEqual Option(StatusCodes.NotFound)
  }
}
