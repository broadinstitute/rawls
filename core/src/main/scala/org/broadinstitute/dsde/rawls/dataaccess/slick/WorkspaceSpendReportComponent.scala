package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.time.LocalDateTime
import org.broadinstitute.dsde.rawls.model._

import scala.language.postfixOps


case class WorkspaceSpendReportRecord (
  id: Long,
  googleProjectId: String,
  reportStartDate: LocalDateTime,
  reportEndDate: LocalDateTime,
  totalCompute: Option[Float],
  totalStorage: Option[Float],
  otherSpend: Option[Float],
  isDataAvailable: Boolean
)

object WorkspaceSpendReportRecord {

  def fromWorkspaceSpendReport(workspaceSpendReport: WorkspaceSpendReport): WorkspaceSpendReportRecord =
    WorkspaceSpendReportRecord(
      workspaceSpendReport.id,
      workspaceSpendReport.googleProjectId,
      workspaceSpendReport.reportStartDate,
      workspaceSpendReport.reportEndDate,
      workspaceSpendReport.totalCompute,
      workspaceSpendReport.totalStorage,
      workspaceSpendReport.otherSpend,
      workspaceSpendReport.isDataAvailable
    )

  def toWorkspaceSpendReport(record: WorkspaceSpendReportRecord): WorkspaceSpendReport =
    WorkspaceSpendReport(
      record.id,
      record.googleProjectId,
      record.reportStartDate,
      record.reportEndDate,
      record.totalCompute,
      record.totalStorage,
      record.otherSpend,
      record.isDataAvailable
    )
}

trait WorkspaceSpendReportComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceSpendReportTable(tag: Tag) extends Table[WorkspaceSpendReportRecord](tag, "WORKSPACE_SPEND_REPORT") {

    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)

    def googleProjectId = column[String]("GOOGLE_PROJECT_ID", O.Length(254))

    def reportStartDate = column[LocalDateTime]("REPORT_START_DATE", O.SqlType("DATETIME"))

    def reportEndDate = column[LocalDateTime]("REPORT_END_DATE", O.SqlType("DATETIME"))

    def totalCompute = column[Option[Float]]("TOTAL_COMPUTE", O.SqlType("NUMBER(10,2)"))

    def totalStorage = column[Option[Float]]("TOTAL_STORAGE", O.SqlType("NUMBER(10,2)"))

    def otherSpend = column[Option[Float]]("OTHER_SPEND", O.SqlType("NUMBER(10,2)"))

    def isDataAvailable = column[Boolean]("IS_DATA_AVAILABLE")

    def * = (id,
             googleProjectId,
             reportStartDate,
             reportEndDate,
             totalCompute,
             totalStorage,
             otherSpend,
             isDataAvailable,
    ) <> ((WorkspaceSpendReportRecord.apply _).tupled, WorkspaceSpendReportRecord.unapply)

  }

  protected val workspaceSpendReportQuery = TableQuery[WorkspaceSpendReportTable]

  type WorkspaceSpendReportQueryType = driver.api.Query[WorkspaceSpendReportTable, WorkspaceSpendReportRecord, Seq]

  object WorkspaceSpendReportQuery extends TableQuery(new WorkspaceSpendReportTable(_)) {

    def getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds: Seq[String],
                                                         startDate: LocalDateTime,
                                                         endDate: LocalDateTime
                                            ): ReadAction[Seq[WorkspaceSpendReport]] =
      loadWorkspaceSpendReport(filterByProjectIdsAndReportDate(projectIds, startDate, endDate))


    def filterByProjectIdsAndReportDate(projectIds: Seq[String],
                                startDate: LocalDateTime,
                                endDate: LocalDateTime
                               ): ReadAction[Seq[WorkspaceSpendReportRecord]] =
      workspaceSpendReportQuery
        .filter(x => x.googleProjectId.inSetBind(projectIds.map(_.value)) && x.reportStartDate === startDate && x.reportEndDate === endDate)
        .result

    private def loadWorkspaceSpendReport(lookup: WorkspaceSpendReportQueryType): ReadAction[Seq[WorkspaceSpendReport]] =
      for {
        records <- lookup.result
      } yield records.map { record =>
        WorkspaceSpendReportRecord.toWorkspaceSpendReport(record)
      }

    def insert(workspaceSpendReport: WorkspaceSpendReport): WriteAction[Long] =
      workspaceSpendReportQuery returning workspaceSpendReportQuery.map(_.id) += WorkspaceSpendReportRecord.fromWorkspaceSpendReport(workspaceSpendReport)
  }

}
