package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CloningFailed
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{doAnswer, doNothing, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CloneWorkspaceCreateWDSAppStepSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()
  val sourceWorkspaceId = UUID.randomUUID()
  val wsCreatedDate: DateTime = DateTime.parse("2023-01-18T10:08:48.541-05:00")

  behavior of "Creating a WDS instance in the cloned workspace"

  it should "complete without taking any actions if automatic app creation is disabled on the job" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.DISABLE_AUTOMATIC_APP_CREATION_KEY -> "true")),
      JobType.CreateWdsAppInClonedWorkspace
    )
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val job: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      job.jobType shouldBe JobType.CloneWorkspaceContainerInit
      Future.successful()
    }.when(monitorRecordDao).create(ArgumentMatchers.any[WorkspaceManagerResourceMonitorRecord]())

    val step = new CloneWorkspaceCreateWDSAppStep(
      mock[LeonardoDAO],
      mock[WorkspaceRepository],
      monitorRecordDao,
      workspaceId,
      monitorRecord
    )
    whenReady(step.runStep(mock[RawlsRequestContext]))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(monitorRecordDao).create(ArgumentMatchers.any())
  }

  it should "start WDS in Leo" in {
    val args = Map(WorkspaceCloningRunner.SOURCE_WORKSPACE_KEY -> sourceWorkspaceId.toString)
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(args),
      JobType.CreateWdsAppInClonedWorkspace
    )
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val job: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      job.jobType shouldBe JobType.CloneWorkspaceContainerInit
      Future.successful()
    }.when(monitorRecordDao).create(ArgumentMatchers.any[WorkspaceManagerResourceMonitorRecord]())
    val token = "test"
    val userInfo = UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(token), 100, mock[RawlsUserSubjectId], None)
    val ctx = RawlsRequestContext(userInfo, None)

    val leonardoDAO = mock[LeonardoDAO]
    doNothing.when(leonardoDAO).createWDSInstance(token, workspaceId, Some(sourceWorkspaceId))

    val step = new CloneWorkspaceCreateWDSAppStep(
      leonardoDAO,
      mock[WorkspaceRepository],
      monitorRecordDao,
      workspaceId,
      monitorRecord
    )
    whenReady(step.runStep(ctx))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(leonardoDAO).createWDSInstance(token, workspaceId, Some(sourceWorkspaceId))
    verify(monitorRecordDao).create(ArgumentMatchers.any())
  }

  it should "record the error when starting WDS fails" in {
    val args = Map(WorkspaceCloningRunner.SOURCE_WORKSPACE_KEY -> sourceWorkspaceId.toString)
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(args),
      JobType.CreateWdsAppInClonedWorkspace
    )
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val job: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      job.jobType shouldBe JobType.CloneWorkspaceContainerInit
      Future.successful()
    }.when(monitorRecordDao).create(ArgumentMatchers.any[WorkspaceManagerResourceMonitorRecord]())
    val token = "test"
    val userInfo = UserInfo(RawlsUserEmail(userEmail), OAuth2BearerToken(token), 100, mock[RawlsUserSubjectId], None)
    val ctx = RawlsRequestContext(userInfo, None)

    val leonardoDAO = mock[LeonardoDAO]
    doAnswer { _ =>
      throw new ApiException
    }.when(leonardoDAO).createWDSInstance(token, workspaceId, Some(sourceWorkspaceId))

    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.setFailedState(ArgumentMatchers.eq(workspaceId),
                                         ArgumentMatchers.eq(CloningFailed),
                                         anyString()
      )
    ).thenReturn(Future(1))
    val step = new CloneWorkspaceCreateWDSAppStep(
      leonardoDAO,
      workspaceRepository,
      monitorRecordDao,
      workspaceId,
      monitorRecord
    )
    whenReady(step.runStep(ctx))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(leonardoDAO).createWDSInstance(token, workspaceId, Some(sourceWorkspaceId))
    verify(workspaceRepository).setFailedState(
      ArgumentMatchers.eq(workspaceId),
      ArgumentMatchers.eq(CloningFailed),
      anyString()
    )

  }

}
