package org.broadinstitute.dsde.rawls.spendreporting

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceSpendReportRecord
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, SpendReportingResults, WorkspaceName, WorkspaceSpendReport}

import java.time.LocalDateTime
import scala.concurrent.Future

/**
 * Data access for workspace spend report data
 */
class WorkspaceSpendReportRepository(dataSource: SlickDataSource) {

  def getWorkspaceSpendReports(projectIds: Set[String],
                               startDate: LocalDateTime,
                               endDate: LocalDateTime
  ): Future[Seq[WorkspaceSpendReport]] =
    dataSource.inTransaction(
      _.WorkspaceSpendReportQuery.getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds, startDate, endDate)
    )

  def insertSpendReportResults(spendReportingResults: SpendReportingResults): Seq[Future[Long]] =
    WorkspaceSpendReportRecord
      .fromSpendReportingResults(spendReportingResults)
      .map(workspaceSpendReport => insertWorkspaceSpendReport(workspaceSpendReport))

  def insertWorkspaceSpendReport(workspaceSpendReport: WorkspaceSpendReport): Future[Long] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.WorkspaceSpendReportQuery.insert(workspaceSpendReport)
    }

}
