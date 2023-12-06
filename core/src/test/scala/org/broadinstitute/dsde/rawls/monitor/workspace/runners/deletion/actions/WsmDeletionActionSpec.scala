package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{JobReport, JobResult}
import org.broadinstitute.dsde.rawls.TestExecutionContext
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
import javax.ws.rs.ProcessingException
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}

class WsmDeletionActionSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

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
  behavior of "setup"

  it should "start workspace deletion in WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    val action = new WsmDeletionAction(wsmDao)

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
    val action = new WsmDeletionAction(wsmDao)

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
    val action = new WsmDeletionAction(wsmDAO)

    Await.result(action.startStep(azureWorkspace, jobId.toString, ctx), Duration.Inf)

    verify(wsmDAO, times(2)).deleteWorkspaceV2(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                               ArgumentMatchers.eq(jobId.toString),
                                               ArgumentMatchers.eq(ctx)
    )
  }

  behavior of "polling for completion"

  it should "succeed with true on a completed job from WSM" in {
    val jobId = UUID.randomUUID()
    val result = new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenReturn(result)

    val action = new WsmDeletionAction(wsmDao)

    Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf) shouldBe true

    verify(wsmDao).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                              ArgumentMatchers.eq(jobId.toString),
                                              ArgumentMatchers.eq(ctx)
    )
  }

  it should "succeed with false on a running job from WSM" in {
    val jobId = UUID.randomUUID()
    val result = new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING))
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenReturn(result)

    val action = new WsmDeletionAction(wsmDao)

    Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf) shouldBe false

    verify(wsmDao).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                              ArgumentMatchers.eq(jobId.toString),
                                              ArgumentMatchers.eq(ctx)
    )
  }

  it should "fail with on a failed job from WSM" in {
    val jobId = UUID.randomUUID()
    val result = new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.FAILED))
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenReturn(result)

    val action = new WsmDeletionAction(wsmDao)

    intercept[WorkspaceDeletionActionFailureException] {
      Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf)
    }

    verify(wsmDao).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                              ArgumentMatchers.eq(jobId.toString),
                                              ArgumentMatchers.eq(ctx)
    )
  }

  it should "succeed on a 403 from WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
    )
    val action = new WsmDeletionAction(wsmDao)

    Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf) shouldBe true

    verify(wsmDao).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                              ArgumentMatchers.eq(jobId.toString),
                                              ArgumentMatchers.eq(ctx)
    )
  }

  it should "retry a 500 from WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ApiException(StatusCodes.GatewayTimeout.intValue, "nope"))
      .thenAnswer(_ => throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden"))

    val action = new WsmDeletionAction(wsmDao)

    Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf) shouldBe true

    verify(wsmDao, times(2)).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                                        ArgumentMatchers.eq(jobId.toString),
                                                        ArgumentMatchers.eq(ctx)
    )
  }

  it should "retry on ProcessingExceptions when connecting to WSM" in {
    val jobId = UUID.randomUUID()
    val wsmDao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDao.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ProcessingException("processing exception"))
      .thenReturn(new JobResult().jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED)))

    val action = new WsmDeletionAction(wsmDao)

    Await.result(action.pollForCompletion(azureWorkspace, jobId.toString, ctx), Duration.Inf) shouldBe true

    verify(wsmDao, times(2)).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                                        ArgumentMatchers.eq(jobId.toString),
                                                        ArgumentMatchers.eq(ctx)
    )
  }

}
