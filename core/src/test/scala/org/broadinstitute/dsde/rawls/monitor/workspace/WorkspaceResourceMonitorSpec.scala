package org.broadinstitute.dsde.rawls.monitor.workspace

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
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
      Timestamp.from(Instant.now())
    )
    val job1 = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId1"),
      Timestamp.from(Instant.now())
    )
    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(jobDao.selectAll()).thenReturn(Future.successful(Seq(job0, job1)))
    val monitor = spy(new WorkspaceResourceMonitor(jobDao, List()))
    doReturn(Future.successful(true)).when(monitor).runJob(ArgumentMatchers.eq(job0))
    doReturn(Future.successful(false)).when(monitor).runJob(ArgumentMatchers.eq(job1))

    Await.result(monitor.checkJobs(), Duration.Inf) shouldBe CheckDone(1)
  }

  behavior of "WorkspaceResourceMonitor.runJob"

  it should "delete a job after it completes successfully" in {
    val runner = spy(new WorkspaceManagerResourceJobRunner {
      override val jobType: JobType = JobType.AzureLandingZoneResult
      override def run(job: WorkspaceManagerResourceMonitorRecord)(implicit
        executionContext: ExecutionContext
      ): Future[Boolean] = Future.successful(true)
    })
    val job = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId"),
      Timestamp.from(Instant.now())
    )
    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(jobDao.selectAll()).thenReturn(Future.successful(Seq(job)))
    when(jobDao.delete(ArgumentMatchers.any())).thenReturn(Future.successful(true))
    val monitor = new WorkspaceResourceMonitor(jobDao, List(runner))

    Await.result(monitor.runJob(job), Duration.Inf) shouldBe true
    verify(jobDao).delete(ArgumentMatchers.any())

  }

  it should "call all job runners registered for a job type" in {
    val runner0 = spy(new WorkspaceManagerResourceJobRunner {
      override val jobType: JobType = JobType.AzureLandingZoneResult
      override def run(job: WorkspaceManagerResourceMonitorRecord)(implicit
        executionContext: ExecutionContext
      ): Future[Boolean] = Future.successful(true)
    })
    val runner1 = spy(new WorkspaceManagerResourceJobRunner {
      override val jobType: JobType = JobType.AzureLandingZoneResult
      override def run(job: WorkspaceManagerResourceMonitorRecord)(implicit
        executionContext: ExecutionContext
      ): Future[Boolean] = Future.successful(false)
    })
    val job = new WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      None,
      Some("bpId"),
      Timestamp.from(Instant.now())
    )
    val jobDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(jobDao.selectAll()).thenReturn(Future.successful(Seq(job)))
    val monitor = new WorkspaceResourceMonitor(jobDao, List(runner0, runner1))

    Await.result(monitor.runJob(job), Duration.Inf) shouldBe false
    verify(runner0).run(job)
    verify(runner1).run(job)
  }

}
