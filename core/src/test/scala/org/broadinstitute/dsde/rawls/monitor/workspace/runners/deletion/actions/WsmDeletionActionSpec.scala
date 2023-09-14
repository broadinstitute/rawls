package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{JobReport, JobResult}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.mock.MockWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
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

class WsmDeletionActionSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  private val pollInterval = FiniteDuration(1, TimeUnit.SECONDS)
  private val timeout = FiniteDuration(3, TimeUnit.SECONDS)
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit val actorSystem: ActorSystem = ActorSystem("WsmDeletionActionSpec")
  private val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                                  OAuth2BearerToken("token"),
                                  123,
                                  RawlsUserSubjectId("123456789876543212345")
  )
  private val azureWorkspace: Workspace = Workspace.buildReadyMcWorkspace(
    "fake_azure_bp",
    "fake_ws",
    UUID.randomUUID().toString,
    DateTime.now(),
    DateTime.now(),
    "example@example.com",
    Map.empty
  )
  private val ctx = RawlsRequestContext(userInfo)
  val monitorRecord: WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forWorkspaceDeletion(
      UUID.randomUUID(),
      azureWorkspace.workspaceIdAsUUID,
      RawlsUserEmail("example@example.com")
    )

  behavior of "startStep"

  it should "start workspace deletion in WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    val action = new WsmDeletionAction(wsmDao, pollInterval, timeout)

    Await.result(action.startStep(azureWorkspace, jobId.toString, ctx), Duration.Inf)

    verify(wsmDao).deleteWorkspaceV2(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                     ArgumentMatchers.eq(jobId.toString),
                                     ArgumentMatchers.eq(ctx)
    )
  }

  it should "succeed on a 404 from WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.deleteWorkspaceV2(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.NotFound.intValue, "not found")
    )
    val action = new WsmDeletionAction(wsmDao, pollInterval, timeout)

    Await.result(action.startStep(azureWorkspace, jobId.toString, ctx), Duration.Inf)

    verify(wsmDao).deleteWorkspaceV2(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                     ArgumentMatchers.eq(jobId.toString),
                                     ArgumentMatchers.eq(ctx)
    )
  }

  it should "retry on 5xx from WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDAO: WorkspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0

      override def deleteWorkspaceV2(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult = {
        times = times + 1

        if (times <= 1) {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }

        new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING))
      }
    })
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    Await.result(action.startStep(azureWorkspace, jobId.toString, ctx), Duration.Inf)

    verify(wsmDAO, times(2)).deleteWorkspaceV2(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                               ArgumentMatchers.eq(jobId.toString),
                                               ArgumentMatchers.eq(ctx)
    )
  }

  it should "fail for other exceptions" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.deleteWorkspaceV2(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(_ =>
      throw new IllegalStateException("failed")
    )
    val action = new WsmDeletionAction(wsmDao, pollInterval, timeout)

    intercept[IllegalStateException] {
      Await.result(action.startStep(azureWorkspace, jobId.toString, ctx), Duration.Inf)
    }
  }

  behavior of "isComplete"

  it should "return true if the operation is complete" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenReturn(new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED)))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    val result = Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)

    result shouldBe true
  }

  it should "return false if the operation is incomplete" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenReturn(new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING)))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    val result = Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)

    result shouldBe false
  }

  it should "return true if WSM responds with a 403" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenAnswer(_ => throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden"))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    val result = Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)

    result shouldBe true
  }

  it should "fail if the WSM job failed" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenReturn(new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.FAILED)))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    intercept[WorkspaceDeletionActionFailureException] {
      Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)
    }
  }

  it should "fail for other WSM API errors" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenAnswer(_ => throw new ApiException(StatusCodes.ImATeapot.intValue, "teapot"))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    intercept[ApiException] {
      Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)
    }
  }

  it should "fail for other exceptions" in {
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(
      wsmDAO.getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
                                        ArgumentMatchers.eq(ctx)
      )
    )
      .thenAnswer(_ => throw new IllegalStateException("failed"))
    val action = new WsmDeletionAction(wsmDAO, pollInterval, timeout)

    intercept[IllegalStateException] {
      Await.result(action.isComplete(azureWorkspace, monitorRecord, ctx), Duration.Inf)
    }
  }

  it should "fail if the job timeout is exceeded" in {
    val jobId = UUID.randomUUID().toString
    val checkTime = DateTime.now
    val action = new WsmDeletionAction(mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS), pollInterval, timeout)

    intercept[WorkspaceDeletionActionTimeoutException] {
      Await.result(action.isComplete(azureWorkspace, jobId, checkTime.minusSeconds(10), checkTime, ctx), Duration.Inf)
    }
  }
}
