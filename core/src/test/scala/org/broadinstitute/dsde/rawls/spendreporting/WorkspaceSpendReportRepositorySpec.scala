package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.WorkspaceSpendReport
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.time.LocalDateTime
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WorkspaceSpendReportRepositorySpec
    extends AnyFlatSpec
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with TestDriverComponent {

  behavior of "getWorkspace"

  val projectId = "fake-google-project"
  val projectIds: Set[String] = Set(projectId)
  val startDate: LocalDateTime = LocalDateTime.now().minusDays(30)
  val endDate: LocalDateTime = LocalDateTime.now()
  def makeWorkspaceSpendReport(projectId: String): WorkspaceSpendReport = WorkspaceSpendReport.newWorkspaceSpendReport(
    projectId,
    startDate,
    endDate,
    "USD",
    isDataAvailable = true,
    Option(100.00f),
    Option(50.00f),
    Option(5.00f),
    Option(0.00f),
    Option(0.00f),
    Option(0.00f)
  )

  it should "get workspace spend reports for given report date if present" in {
    val repo = new WorkspaceSpendReportRepository(slickDataSource)
    val report: WorkspaceSpendReport = makeWorkspaceSpendReport(projectId)
    Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)

    val results = Await.result(repo.getWorkspaceSpendReports(projectIds, startDate, endDate), Duration.Inf)
    assertResult(1)(results.size)
    results.map { result =>
      assertResult(projectId)(result.googleProjectId)
    }
  }

  it should "return none if no workspace spend reports present" in {
    val repo = new WorkspaceSpendReportRepository(slickDataSource)
    val result = Await.result(repo.getWorkspaceSpendReports(projectIds, startDate, endDate), Duration.Inf)
    assertResult(Seq())(result)
  }

  behavior of "insertSpendReport"

  it should "error if insert violates unique constraint on google project id, start and end date" in {
    val repo = new WorkspaceSpendReportRepository(slickDataSource)
    val report: WorkspaceSpendReport = makeWorkspaceSpendReport(projectId)
    val success = Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)
    val thrown = intercept[java.sql.SQLIntegrityConstraintViolationException] {
      Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)
    }
    thrown.getErrorCode shouldBe Some(StatusCodes.Conflict)
  }
}
