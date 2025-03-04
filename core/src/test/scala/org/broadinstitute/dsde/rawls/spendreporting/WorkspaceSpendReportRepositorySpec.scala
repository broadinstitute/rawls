package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.WorkspaceSpendReport
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.time.LocalDateTime
import java.util.UUID
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
  val endDate: LocalDateTime = LocalDateTime.of(2025, 3, 4, 0, 0, 0)
  val startDate: LocalDateTime = endDate.minusDays(30)
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
    val noResults = Await.result(repo.getWorkspaceSpendReports(projectIds, startDate, endDate), Duration.Inf)
    assertResult(0)(noResults.size)

    val report: WorkspaceSpendReport = makeWorkspaceSpendReport(projectId)
    Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)

    val results = Await.result(repo.getWorkspaceSpendReports(projectIds, startDate, endDate), Duration.Inf)
    assertResult(1)(results.size)
    results.map { result =>
      assertResult(projectId)(result.googleProjectId)
      assertResult(startDate)(result.reportStartDate)
      assertResult(endDate)(result.reportEndDate)
    }
  }

  behavior of "insertSpendReport"

  it should "error if insert violates unique constraint on google project id, start and end date" in {
    val repo = new WorkspaceSpendReportRepository(slickDataSource)
    val report: WorkspaceSpendReport = makeWorkspaceSpendReport(UUID.randomUUID().toString)
    Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)
    intercept[java.sql.SQLIntegrityConstraintViolationException] {
      Await.result(repo.insertWorkspaceSpendReport(report), Duration.Inf)
    }
  }
}
