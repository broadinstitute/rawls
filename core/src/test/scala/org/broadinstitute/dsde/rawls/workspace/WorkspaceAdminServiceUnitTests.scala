package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamAdminDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamFullyQualifiedResourceId,
  SamResourceTypeAdminActions,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamWorkspacePolicyNames,
  UserInfo,
  Workspace,
  WorkspaceAdminResponse,
  WorkspaceDetails,
  WorkspaceName,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{times, verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

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

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service =
      workspaceAdminServiceConstructor(samDAO = samDAO,
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
    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(false))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service = workspaceAdminServiceConstructor(samDAO = samDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspaceById(UUID.randomUUID()), Duration.Inf)
    }
    exception.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

  it should "throw if the workspace is not found" in {
    val workspaceId = workspace.workspaceIdAsUUID

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceId)).thenReturn(Future.successful(None))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service =
      workspaceAdminServiceConstructor(samDAO = samDAO, workspaceRepository = workspaceRepository)

    intercept[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceById(workspaceId), Duration.Inf)
    }
  }

  "adminDeleteMcWorkspace" should "delete an MC workspace if the user is an admin" in {
    val workspaceName = WorkspaceName("test-namespace", "test-name")
    val workspaceWithMcType = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val userEmail = defaultRequestContext.userInfo.userEmail.value

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName)).thenReturn(Future.successful(Option(workspaceWithMcType)))
    when(workspaceRepository.deleteWorkspace(workspaceName)).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(ArgumentMatchers.any())).thenReturn(Future.successful(true))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.addUserToPolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.eq(SamWorkspacePolicyNames.owner),
        ArgumentMatchers.eq(userEmail),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))
    when(
      samDAO.deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    val service = workspaceAdminServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      gcsDAO = gcsDAO
    )

    Await.result(service.adminDeleteMcWorkspace(workspaceName), Duration.Inf)

    // Verify user was added to the owner policy
    verify(samAdminDAO).addUserToPolicy(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
      ArgumentMatchers.eq(SamWorkspacePolicyNames.owner),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.any()
    )

    // Verify deletion steps
    verify(workspaceRepository).deleteWorkspace(workspaceName)
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
      ArgumentMatchers.any()
    )
  }

  it should "throw if the workspace is not an MC workspace" in {
    val workspaceName = WorkspaceName("test-namespace", "test-name")
    val workspaceWithRawlsType = workspace.copy(workspaceType = WorkspaceType.RawlsWorkspace)

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName)).thenReturn(Future.successful(Option(workspaceWithRawlsType)))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(ArgumentMatchers.any())).thenReturn(Future.successful(true))

    val service = workspaceAdminServiceConstructor(
      workspaceRepository = workspaceRepository,
      gcsDAO = gcsDAO
    )

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.adminDeleteMcWorkspace(workspaceName), Duration.Inf)
    }

    exception.errorReport.statusCode shouldEqual Some(StatusCodes.BadRequest)
  }

  it should "throw if the workspace does not exist" in {
    val workspaceName = WorkspaceName("test-namespace", "test-name")

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName)).thenReturn(Future.successful(None))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(ArgumentMatchers.any())).thenReturn(Future.successful(true))

    val service = workspaceAdminServiceConstructor(
      workspaceRepository = workspaceRepository,
      gcsDAO = gcsDAO
    )

    intercept[NoSuchWorkspaceException] {
      Await.result(service.adminDeleteMcWorkspace(workspaceName), Duration.Inf)
    }
  }

  it should "throw if the user is not an admin" in {
    val workspaceName = WorkspaceName("test-namespace", "test-name")

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(ArgumentMatchers.any())).thenReturn(Future.successful(false))

    val service = workspaceAdminServiceConstructor(gcsDAO = gcsDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.adminDeleteMcWorkspace(workspaceName), Duration.Inf)
    }

    exception.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

  it should "continue deleting the workspace when Sam API calls throw 404 errors" in {
    val workspaceName = WorkspaceName("test-namespace", "test-name")
    val workspaceWithMcType = workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val userEmail = defaultRequestContext.userInfo.userEmail.value

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName)).thenReturn(Future.successful(Option(workspaceWithMcType)))
    when(workspaceRepository.deleteWorkspace(workspaceName)).thenReturn(Future.successful(true))

    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.isAdmin(ArgumentMatchers.any())).thenReturn(Future.successful(true))

    // Mock addUserToPolicy to throw a 404 error
    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.addUserToPolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.eq(SamWorkspacePolicyNames.owner),
        ArgumentMatchers.eq(userEmail),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      Future.failed(
        new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound, "Resource not found when adding user to policy")
        )
      )
    )

    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    // Mock listResourceChildren to throw a 404 error
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      Future.failed(
        new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound, "Resource not found when listing children")
        )
      )
    )

    // Mock deleteResource to throw a 404 error
    when(
      samDAO.deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      Future.failed(
        new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound, "Resource not found when deleting")
        )
      )
    )

    val service = workspaceAdminServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository,
      gcsDAO = gcsDAO
    )

    // The operation should complete successfully despite the 404 errors
    Await.result(service.adminDeleteMcWorkspace(workspaceName), Duration.Inf)

    // Verify addUserToPolicy was called
    verify(samAdminDAO).addUserToPolicy(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
      ArgumentMatchers.eq(SamWorkspacePolicyNames.owner),
      ArgumentMatchers.eq(userEmail),
      ArgumentMatchers.any()
    )

    // Verify listResourceChildren was called
    verify(samDAO).listResourceChildren(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
      ArgumentMatchers.any()
    )

    // Verify deleteResource was called
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(workspaceWithMcType.workspaceId),
      ArgumentMatchers.any()
    )

    // Verify deleteWorkspace in the repository was called and completed
    verify(workspaceRepository).deleteWorkspace(workspaceName)
  }

  "recursivelyDeleteSamResource" should "delete a resource and its children recursively" in {
    // Create a mock SamDAO with resource children
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString

    // Use SamFullyQualifiedResourceId instead of custom case class
    val childResource1 = SamFullyQualifiedResourceId("child-id-1", "child-type-1")
    val childResource2 = SamFullyQualifiedResourceId("child-id-2", "child-type-2")
    val grandchildResource = SamFullyQualifiedResourceId("grandchild-id", "grandchild-type")

    val samDAO = mock[SamDAO]

    // Mock the first level of children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List(childResource1, childResource2)))

    // Mock the second level (grandchildren) - first child has a child, second child doesn't
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource1.resourceTypeName)),
        ArgumentMatchers.eq(childResource1.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List(grandchildResource)))

    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource2.resourceTypeName)),
        ArgumentMatchers.eq(childResource2.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock the third level (no children)
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(grandchildResource.resourceTypeName)),
        ArgumentMatchers.eq(grandchildResource.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock all deleteResource calls to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    val service = workspaceAdminServiceConstructor(samDAO = samDAO)

    // Call the method under test
    Await.result(service.recursivelyDeleteSamResource(resourceTypeName, resourceId, defaultRequestContext),
                 Duration.Inf
    )

    // Verify that deleteResource was called for all resources in the correct order (bottom-up)
    // First verify the grandchild was deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(grandchildResource.resourceTypeName)),
      ArgumentMatchers.eq(grandchildResource.resourceId),
      ArgumentMatchers.any()
    )

    // Then verify both children were deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(childResource1.resourceTypeName)),
      ArgumentMatchers.eq(childResource1.resourceId),
      ArgumentMatchers.any()
    )

    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(childResource2.resourceTypeName)),
      ArgumentMatchers.eq(childResource2.resourceId),
      ArgumentMatchers.any()
    )

    // Finally verify the parent resource was deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }

  it should "handle empty child resources" in {
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString

    val samDAO = mock[SamDAO]

    // Mock no children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock deleteResource to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    val service = workspaceAdminServiceConstructor(samDAO = samDAO)

    // Call the method under test
    Await.result(service.recursivelyDeleteSamResource(resourceTypeName, resourceId, defaultRequestContext),
                 Duration.Inf
    )

    // Verify that deleteResource was called only for the parent resource
    verify(samDAO, times(1)).deleteResource(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )

    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }

  "getWorkspaceId" should "return the workspace ID if the user is an admin and the workspace exists" in {
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspaceId(workspaceName))
      .thenReturn(Future.successful(Option(workspace.workspaceIdAsUUID)))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service = workspaceAdminServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository
    )

    val result = Await.result(service.getWorkspaceId(workspaceName), Duration.Inf)
    result shouldEqual Option(workspace.workspaceId)
  }

  it should "return None if the workspace does not exist" in {
    val workspaceName = workspace.toWorkspaceName

    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspaceId(workspaceName)).thenReturn(Future.successful(None))

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service = workspaceAdminServiceConstructor(
      samDAO = samDAO,
      workspaceRepository = workspaceRepository
    )

    val result = Await.result(service.getWorkspaceId(workspaceName), Duration.Inf)
    result shouldEqual None
  }

  it should "throw if the user is not an admin" in {
    val workspaceName = workspace.toWorkspaceName

    val samAdminDAO = mock[SamAdminDAO]
    when(
      samAdminDAO.userHasResourceTypeAdminPermission(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(SamResourceTypeAdminActions.readSummaryInformation),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(false))
    val samDAO = mock[SamDAO]
    when(samDAO.admin).thenReturn(samAdminDAO)

    val service = workspaceAdminServiceConstructor(samDAO = samDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspaceId(workspaceName), Duration.Inf)
    }
    exception.errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
  }

  it should "handle 403 Forbidden errors when listing resource children" in {
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString
    val childResource = SamFullyQualifiedResourceId("child-id", "child-type")

    val samDAO = mock[SamDAO]

    // Mock a 403 Forbidden error for the parent resource's children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      Future.failed(
        new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "Forbidden"))
      )
    )

    // Mock successful listing for the child resource (this should never be called)
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource.resourceTypeName)),
        ArgumentMatchers.eq(childResource.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock deleteResource to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    val service = workspaceAdminServiceConstructor(samDAO = samDAO)

    // Call the method under test - should not throw an exception
    Await.result(service.recursivelyDeleteSamResource(resourceTypeName, resourceId, defaultRequestContext),
                 Duration.Inf
    )

    // Verify that listResourceChildren was called for the parent resource
    verify(samDAO).listResourceChildren(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )

    // Verify that no other listResourceChildren calls were made
    verify(samDAO, times(1)).listResourceChildren(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )

    // Verify that deleteResource was called only for the parent resource
    verify(samDAO, times(1)).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }
}
