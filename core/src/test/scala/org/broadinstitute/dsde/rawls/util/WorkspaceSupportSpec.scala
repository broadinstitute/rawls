package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.{
  NoSuchWorkspaceException,
  TestExecutionContext,
  UserDisabledException,
  WorkspaceAccessDeniedException
}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceAction,
  SamResourceTypeName,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.joda.time.DateTime
import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock
import org.mockito.ArgumentMatchers._
import org.mockito.ArgumentMatchers.{eq => mockeq}
import org.mockito.Mockito
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class WorkspaceSupportSpec extends AnyFlatSpec with Matchers {

  private val atMost = Duration("60 seconds") // timeout for Await() in tests

  private val defaultWorkspaceName = WorkspaceName("default-namespace", "default-name")

  private val defaultWorkspace = Workspace(
    defaultWorkspaceName.namespace,
    defaultWorkspaceName.name,
    UUID.randomUUID().toString,
    "aBucket2",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "testUser",
    Map()
  )

  private val defaultUserStatus = SamUserStatusResponse(userSubjectId = "123", userEmail = "test", enabled = true)

  behavior of "getV2WorkspaceContextAndPermissions"

  // successful case
  it should "return workspace if all checks pass" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exists
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user has permission and is enabled
    when(samDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext]))
      .thenReturn(Future.successful(true))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    val actual =
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)

    actual shouldBe defaultWorkspace

    // successful case should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // successful case should only call Sam once
    verify(samDAO, times(1)).userHasAction(any[SamResourceTypeName],
                                           any[String],
                                           any[SamResourceAction],
                                           any[RawlsRequestContext]
    )
    verifyNoMoreInteractions(samDAO)
  }

  // error cases where only one check fails
  it should "throw UserDisabledException if user is not found" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exists
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user does not exist
    when(samDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext]))
      .thenAnswer(_ => Future.failed(new ApiException(StatusCodes.Forbidden.intValue, "Azure Id 123 not found in sam")))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[UserDisabledException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once
    verify(samDAO, times(1)).userHasAction(any[SamResourceTypeName],
                                           any[String],
                                           any[SamResourceAction],
                                           any[RawlsRequestContext]
    )
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw UserDisabledException if user is not enabled" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exists
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user is NOT enabled
    when(samDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext]))
      .thenAnswer(_ => Future.failed(new ApiException(StatusCodes.Unauthorized.intValue, "Message: User is disabled.")))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[UserDisabledException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once
    verify(samDAO, times(1)).userHasAction(any[SamResourceTypeName],
                                           any[String],
                                           any[SamResourceAction],
                                           any[RawlsRequestContext]
    )
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw NoSuchWorkspaceException if workspace does not exist" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace DOES NOT exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(None))
    // user is enabled
    when(samDAO.getUserStatus(any[RawlsRequestContext]))
      .thenReturn(Future.successful(Option(defaultUserStatus)))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[NoSuchWorkspaceException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once, to check if the user is enabled once we discover the
    // workspace is missing
    verify(samDAO, times(1)).getUserStatus(any[RawlsRequestContext])
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw NoSuchWorkspaceException if user cannot read" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user DOES NOT have permission
    when(samDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext]))
      .thenReturn(Future.successful(false))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[NoSuchWorkspaceException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once
    verify(samDAO, times(1)).userHasAction(any[SamResourceTypeName],
                                           any[String],
                                           any[SamResourceAction],
                                           any[RawlsRequestContext]
    )
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw WorkspaceAccessDeniedException if user can read but doesn't have requested permission" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user DOES NOT have write permission
    when(
      samDAO.userHasAction(any[SamResourceTypeName],
                           any[String],
                           mockeq(SamWorkspaceActions.write),
                           any[RawlsRequestContext]
      )
    )
      .thenReturn(Future.successful(false))
    // user DOES have read permission
    when(
      samDAO.userHasAction(any[SamResourceTypeName],
                           any[String],
                           mockeq(SamWorkspaceActions.read),
                           any[RawlsRequestContext]
      )
    )
      .thenReturn(Future.successful(true))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[WorkspaceAccessDeniedException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.write), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should call Sam twice - first to see if the user has write (which returns false),
    // then again to see if the user has read
    val inOrder = Mockito.inOrder(samDAO)
    inOrder
      .verify(samDAO, times(1))
      .userHasAction(any[SamResourceTypeName], any[String], mockeq(SamWorkspaceActions.write), any[RawlsRequestContext])
    inOrder
      .verify(samDAO, times(1))
      .userHasAction(any[SamResourceTypeName], any[String], mockeq(SamWorkspaceActions.read), any[RawlsRequestContext])
    inOrder.verifyNoMoreInteractions()
  }

  // error cases where multiple checks fail
  it should "throw UserDisabledException if user is not enabled and workspace does not exist" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace DOES NOT exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(None))
    // user is NOT enabled
    when(samDAO.getUserStatus(any[RawlsRequestContext]))
      .thenReturn(Future.successful(Option(defaultUserStatus.copy(enabled = false))))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[UserDisabledException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once, to check if the user is enabled once we discover the
    // workspace is missing
    verify(samDAO, times(1)).getUserStatus(any[RawlsRequestContext])
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw NoSuchWorkspaceException if workspace does not exist and user doesn't have permission" in {
    // Note: it is expected that if the workspace does not exist, the user should not have permission to it.
    // So this test isn't a corner case; but it does verify that our implementation logic throws the
    // correct error.
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace DOES NOT exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(None))
    // user is enabled
    when(samDAO.getUserStatus(any[RawlsRequestContext]))
      .thenReturn(Future.successful(Option(defaultUserStatus)))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[NoSuchWorkspaceException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once, to check if the user is enabled once we discover the
    // workspace is missing
    verify(samDAO, times(1)).getUserStatus(any[RawlsRequestContext])
    verifyNoMoreInteractions(samDAO)
  }

  it should "throw NoSuchWorkspaceException if user is not enabled, workspace does not exist, and user doesn't have permission" in {
    // Note: it is expected that if the workspace does not exist, the user should not have permission to it.
    // So this test isn't a corner case; but it does verify that our implementation logic throws the
    // correct error.
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace DOES NOT exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(None))
    // user is NOT enabled
    when(samDAO.getUserStatus(any[RawlsRequestContext]))
      .thenReturn(Future.successful(Option(defaultUserStatus.copy(enabled = false))))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[UserDisabledException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once, to check if the user is enabled once we discover the
    // workspace is missing
    verify(samDAO, times(1)).getUserStatus(any[RawlsRequestContext])
    verifyNoMoreInteractions(samDAO)
  }

  // error cases where a Sam API call fails
  it should "propagate Sam's ApiException if the initial permission check API call fails" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user is enabled
    when(samDAO.getUserStatus(any[RawlsRequestContext]))
      .thenReturn(Future.successful(Option(defaultUserStatus)))
    // permission API call fails
    when(samDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext]))
      .thenAnswer(_ => Future.failed(new ApiException(555, "Unit test mock error")))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[ApiException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.read), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should only call Sam once
    verify(samDAO, times(1)).userHasAction(any[SamResourceTypeName],
                                           any[String],
                                           any[SamResourceAction],
                                           any[RawlsRequestContext]
    )
    verifyNoMoreInteractions(samDAO)
  }

  it should "propagate Sam's ApiException if the fallback permission check API call fails" in {
    val samDAO = mock[SamDAO]
    val workspaceRepository = mock[WorkspaceRepository]
    // workspace exist
    when(workspaceRepository.getWorkspace(mockeq(defaultWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future.successful(Option(defaultWorkspace)))
    // user DOES have write permission
    when(
      samDAO.userHasAction(any[SamResourceTypeName],
                           any[String],
                           mockeq(SamWorkspaceActions.write),
                           any[RawlsRequestContext]
      )
    )
      .thenReturn(Future.successful(false))

    // user DOES NOT have read permission
    when(
      samDAO.userHasAction(any[SamResourceTypeName],
                           any[String],
                           mockeq(SamWorkspaceActions.read),
                           any[RawlsRequestContext]
      )
    )
      .thenAnswer(_ => Future.failed(new ApiException(555, "Unit test mock error")))

    val support = new WorkspaceSupportFixture(samDAO, workspaceRepository)

    intercept[ApiException] {
      Await.result(support.getV2WorkspaceContextAndPermissions(defaultWorkspaceName, SamWorkspaceActions.write), atMost)
    }
    // should only call workspaceRepository once
    verify(workspaceRepository, times(1)).getWorkspace(any[WorkspaceName], any[Option[WorkspaceAttributeSpecs]])
    verifyNoMoreInteractions(workspaceRepository)
    // should call Sam twice - first to see if the user has write (which throws an ApiException),
    // then again to see if the user has read
    val inOrder = Mockito.inOrder(samDAO)
    inOrder
      .verify(samDAO, times(1))
      .userHasAction(any[SamResourceTypeName], any[String], mockeq(SamWorkspaceActions.write), any[RawlsRequestContext])
    inOrder
      .verify(samDAO, times(1))
      .userHasAction(any[SamResourceTypeName], any[String], mockeq(SamWorkspaceActions.read), any[RawlsRequestContext])
    inOrder.verifyNoMoreInteractions()
  }
}

class WorkspaceSupportFixture(val samDAO: SamDAO, val workspaceRepository: WorkspaceRepository)
    extends WorkspaceSupport {
  implicit override protected val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  override protected val ctx: RawlsRequestContext = RawlsRequestContext(
    UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
  )
}
