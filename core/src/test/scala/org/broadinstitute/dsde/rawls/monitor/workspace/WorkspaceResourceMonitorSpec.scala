package org.broadinstitute.dsde.rawls.monitor.workspace

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  SlickEnum,
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.monitor.workspace.WorkspaceResourceMonitor.CheckDone
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doReturn, spy, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant
import java.util
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class WorkspaceResourceMonitorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  implicit val testExecutionContext: TestExecutionContext = new TestExecutionContext()

  behavior of "WorkspaceResourceMonitor.checkJobs"

  it should "return a CheckDone message with the number of uncompleted jobs" in {
    val job0 = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId1"),
      None,
      Timestamp.from(Instant.now())
    )
    val job1 = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId1"),
      None,
      Timestamp.from(Instant.now())
    )
    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(jobDao.selectAll()).thenReturn(Future.successful(Seq(job0, job1)))
    val monitor = spy(new WorkspaceResourceMonitor(jobDao, Map.empty))
    doReturn(Future.successful(true)).when(monitor).runJob(ArgumentMatchers.eq(job0))
    doReturn(Future.successful(false)).when(monitor).runJob(ArgumentMatchers.eq(job1))

    Await.result(monitor.checkJobs(), Duration.Inf) shouldBe CheckDone(1)
  }

  behavior of "WorkspaceResourceMonitor.runJob"

  it should "delete a job after it completes successfully" in {
    val job = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId"),
      None,
      Timestamp.from(Instant.now())
    )

    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(jobDao.selectAll()).thenReturn(Future.successful(Seq(job)))
    when(jobDao.delete(ArgumentMatchers.any())).thenReturn(Future.successful(true))

    val monitor = new WorkspaceResourceMonitor(
      jobDao,
      Map(
        JobType.AzureLandingZoneResult -> new WorkspaceManagerResourceJobRunner {
          override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
            executionContext: ExecutionContext
          ): Future[JobStatus] =
            Future.successful(Complete)
        }
      )
    )

    Await.result(monitor.runJob(job), Duration.Inf) shouldBe Complete
    verify(jobDao).delete(ArgumentMatchers.any())
  }

  it should "mark any jobs that doesnt have a registered handler as incomplete" in {
    val job = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId"),
      None,
      Timestamp.from(Instant.now())
    )

    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doReturn(Future.successful(Seq(job))).when(jobDao).selectAll()

    val monitor = new WorkspaceResourceMonitor(jobDao, Map.empty)
    Await.result(monitor.runJob(job), Duration.Inf) shouldBe Incomplete
  }

}
