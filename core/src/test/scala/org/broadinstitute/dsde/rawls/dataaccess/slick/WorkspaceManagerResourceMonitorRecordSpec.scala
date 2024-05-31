package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobType
}
import org.mockito.Mockito.{verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class WorkspaceManagerResourceMonitorRecordSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "WorkspaceManagerResourceMonitorRecord.retryOrTimeout"

  it should "return incomplete if a full day has not passed since the record was created" in {
    val createTime = Timestamp.from(Instant.now().minus(23, ChronoUnit.HOURS))
    val record = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.CloneWorkspaceInit,
      None,
      None,
      None,
      createTime
    )
    whenReady(record.retryOrTimeout())(_ shouldBe Incomplete)
  }

  it should "return complete if a full day has passed since the record was created" in {
    val createTime = Timestamp.from(Instant.now().minus(25, ChronoUnit.HOURS))
    val record =
      WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(), JobType.CloneWorkspaceInit, None, None, None, createTime)
    whenReady(record.retryOrTimeout())(_ shouldBe Complete)
  }

  it should "call the passed onTimeout function before returning on timeout" in {
    val createTime = Timestamp.from(Instant.now().minus(25, ChronoUnit.HOURS))
    val callbackFn = mock[() => Future[Unit]]
    when(callbackFn()).thenReturn(Future.successful())
    val record =
      WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(), JobType.CloneWorkspaceInit, None, None, None, createTime)

    whenReady(record.retryOrTimeout(callbackFn))(_ shouldBe Complete)
    verify(callbackFn)()
  }

}
