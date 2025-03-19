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
  val endDate: LocalDateTime = LocalDateTime.of(2025, 3, 4, 0, 0, 0)
  val startDate: LocalDateTime = endDate.minusDays(30)

  def testWorkspaceSpendReport(id: Long): WorkspaceSpendReport =
    WorkspaceSpendReport(
      id,
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
    val newRecordId = runAndWait(WorkspaceSpendReportQuery.insert(testWorkspaceSpendReport(-1L)))
    val newRecord = testWorkspaceSpendReport(newRecordId)
    assertResult(Seq(newRecord)) {
      runAndWait(
        WorkspaceSpendReportQuery.getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds, startDate, endDate)
      )
    }
  }

}
