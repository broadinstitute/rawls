package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace,
  WorkspaceAdminResponse,
  WorkspaceDetails
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.Mockito.{when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class WorkspaceAdminServiceUnitTests extends AnyFlatSpec with MockitoTestUtils {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  def workspaceAdminServiceConstructor(
    ctx: RawlsRequestContext = defaultRequestContext,
    workspaceSettingRepository: WorkspaceSettingRepository = mock[WorkspaceSettingRepository](
      RETURNS_SMART_NULLS
    ),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  ): WorkspaceAdminService =
    new WorkspaceAdminService(ctx,
                              mock[SlickDataSource],
                              gcsDAO,
                              samDAO,
                              "metricName",
                              workspaceRepository,
                              workspaceSettingRepository
    )

  val workspace: Workspace = Workspace(
    "settingsTestWorkspace",
    "settingsTestNamespace",
    UUID.randomUUID.toString,
    "bucketName",
    Some("workflowCollection"),
    new DateTime(),
    new DateTime(),
    "creator",
    Map.empty
  )

  "getWorkspaceById" should "return the workspace with its settings if the user is an admin" in {
    val workspaceId = workspace.workspaceIdAsUUID

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceId)).thenReturn(Future.successful(Option(workspace)))

    val workspaceSettingRepository = mock[WorkspaceSettingRepository]
    when(workspaceSettingRepository.getWorkspaceSettings(workspaceId)).thenReturn(Future.successful(List.empty))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(defaultRequestContext.userInfo.userEmail.value)).thenReturn(Future.successful(true))

    val service =
      workspaceAdminServiceConstructor(gcsDAO = gcsDAO,
                                       workspaceRepository = workspaceRepository,
                                       workspaceSettingRepository = workspaceSettingRepository
      )

    val returnedWorkspace = Await.result(service.getWorkspaceById(workspaceId), Duration.Inf)
    returnedWorkspace shouldEqual WorkspaceAdminResponse(
      WorkspaceDetails.fromWorkspaceAndOptions(workspace, None, false),
      List.empty
    )
  }

  it should "throw if the user is not an admin" in {
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(defaultRequestContext.userInfo.userEmail.value)).thenReturn(Future.successful(false))

    val service = workspaceAdminServiceConstructor(gcsDAO = gcsDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspaceById(UUID.randomUUID()), Duration.Inf)
    }
    exception.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

  it should "throw if the workspace is not found" in {
    val workspaceId = workspace.workspaceIdAsUUID

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceId)).thenReturn(Future.successful(None))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(defaultRequestContext.userInfo.userEmail.value)).thenReturn(Future.successful(true))

    val service =
      workspaceAdminServiceConstructor(gcsDAO = gcsDAO, workspaceRepository = workspaceRepository)

    intercept[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceById(workspaceId), Duration.Inf)
    }
  }
}
