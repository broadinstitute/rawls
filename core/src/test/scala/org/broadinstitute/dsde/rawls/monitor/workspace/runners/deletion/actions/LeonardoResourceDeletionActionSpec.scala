package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, MockLeonardoDAO}
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{times, verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}

class LeonardoResourceDeletionActionSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit val actorSystem: ActorSystem = ActorSystem("LeonardoAppDeletionActionSpec")

  private val pollInterval = FiniteDuration(1, TimeUnit.SECONDS)
  private val timeout = FiniteDuration(3, TimeUnit.SECONDS)

  private val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                                  OAuth2BearerToken("token"),
                                  123,
                                  RawlsUserSubjectId("123456789876543212345")
  )

  private val ctx = RawlsRequestContext(userInfo)

  private val azureWorkspace: Workspace = Workspace.buildReadyMcWorkspace(
    "fake_azure_bp",
    "fake_ws",
    UUID.randomUUID().toString,
    DateTime.now(),
    DateTime.now(),
    "example@example.com",
    Map.empty
  )

  behavior of "deleteApps"

  it should "start app deletion" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.deleteApps(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO).deleteApps(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  it should "retry on 5xx from leo on app deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = {
        times = times + 1

        if (times <= 1) {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.deleteApps(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).deleteApps(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  behavior of "deleteRuntimes"

  it should "start runtime deletion" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.deleteRuntimes(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO).deleteAzureRuntimes(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  it should "retry on 5xx from leo on runtime deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = {
        times = times + 1

        if (times <= 1) {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.deleteRuntimes(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).deleteAzureRuntimes(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  behavior of "pollAppDeletion"

  it should "poll to successful app deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] = {
        times = times + 1
        if (times > 1) {
          Seq.empty
        } else {
          Seq(new ListAppResponse())
        }
      }
    })
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).listApps(anyString(), any[UUID])
  }

  it should "fail when after exceeding the app deletion poll timeout" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenReturn(Seq(new ListAppResponse()))
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    intercept[LeonardoOperationFailureException] {
      Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
    }
  }

  it should "retry on 5xx from listapps" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] = {
        times = times + 1
        if (times > 1) {
          Seq.empty
        } else {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).listApps(anyString(), any[UUID])
  }

  it should "complete successfully on 403 forbidden when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
    )
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
  }

  it should "complete successfully on 404 not found when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.NotFound.intValue, "forbidden")
    )
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
  }

  it should "fail on other 4xx when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.ImATeapot.intValue, "teapot")
    )
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    intercept[LeonardoOperationFailureException] {
      Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
    }
  }

  it should "not retry on an unrelated exception" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ => throw new IllegalStateException("exception"))
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    intercept[LeonardoOperationFailureException] {
      Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
    }
  }

  behavior of "pollRuntimeDeletion"

  it should "poll to successful runtime deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] = {
        times = times + 1
        if (times > 1) {
          Seq.empty
        } else {
          Seq(new ListRuntimeResponse())
        }
      }
    })
    val action = new LeonardoResourceDeletionAction(leoDAO, pollInterval, timeout)

    Await.result(action.pollRuntimeDeletion(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).listAzureRuntimes(anyString(), any[UUID])
  }
}
