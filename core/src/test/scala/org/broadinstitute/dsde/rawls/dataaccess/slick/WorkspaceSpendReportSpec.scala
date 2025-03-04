package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.OptionValues

import java.time.LocalDateTime
import scala.language.implicitConversions

class WorkspaceSpendReportSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with WorkspaceSpendReportComponent
    with RawlsTestUtils
    with OptionValues {
  val googleProjectId: String = "test_google_project"
  val projectIds: Set[String] = Set(googleProjectId)
  val startDate: LocalDateTime = LocalDateTime.now().minusDays(30)
  val endDate: LocalDateTime = LocalDateTime.now()
  val workspaceSpendReport: WorkspaceSpendReport = WorkspaceSpendReport.newWorkspaceSpendReport(
    googleProjectId,
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

  "WorkspaceSpendReportComponent" should "insert and get reports" in withEmptyTestDatabase {

    assertResult(Seq()) {
      runAndWait(
        WorkspaceSpendReportQuery.getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds, startDate, endDate)
      )
    }

    assert(runAndWait(WorkspaceSpendReportQuery.insert(workspaceSpendReport)).isInstanceOf[Long])

    assertResult(Seq(workspaceSpendReport)) {
      runAndWait(
        WorkspaceSpendReportQuery.getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds, startDate, endDate)
      )
    }

  }

}
