package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.time.{LocalDateTime, ZoneOffset}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportUtils
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime

import java.sql.Timestamp
import java.util.Currency
import scala.language.{postfixOps, reflectiveCalls}

case class WorkspaceSpendReportRecord(
  id: Long,
  googleProjectId: String,
  reportStartDate: Timestamp,
  reportEndDate: Timestamp,
  currency: String,
  isDataAvailable: Boolean,
  totalCompute: Option[Float],
  totalStorage: Option[Float],
  otherSpend: Option[Float],
  computeCredits: Option[Float],
  storageCredits: Option[Float],
  otherCredits: Option[Float]
)

object WorkspaceSpendReportRecord {

  def fromWorkspaceSpendReport(workspaceSpendReport: WorkspaceSpendReport): WorkspaceSpendReportRecord =
    WorkspaceSpendReportRecord(
      workspaceSpendReport.id,
      workspaceSpendReport.googleProjectId,
      new Timestamp(workspaceSpendReport.reportStartDate.toInstant(ZoneOffset.UTC).toEpochMilli),
      new Timestamp(workspaceSpendReport.reportEndDate.toInstant(ZoneOffset.UTC).toEpochMilli),
      workspaceSpendReport.currency,
      workspaceSpendReport.isDataAvailable,
      workspaceSpendReport.totalCompute,
      workspaceSpendReport.totalStorage,
      workspaceSpendReport.otherSpend,
      workspaceSpendReport.computeCredits,
      workspaceSpendReport.storageCredits,
      workspaceSpendReport.otherCredits
    )

  def toWorkspaceSpendReport(record: WorkspaceSpendReportRecord): WorkspaceSpendReport =
    WorkspaceSpendReport(
      record.id,
      record.googleProjectId,
      LocalDateTime.ofInstant(record.reportStartDate.toInstant, ZoneOffset.UTC),
      LocalDateTime.ofInstant(record.reportEndDate.toInstant, ZoneOffset.UTC),
      record.currency,
      record.isDataAvailable,
      record.totalCompute,
      record.totalStorage,
      record.otherSpend,
      record.computeCredits,
      record.storageCredits,
      record.otherCredits
    )

  def fromSpendReportingResults(spendReportingResults: SpendReportingResults): Seq[WorkspaceSpendReport] = {
    val summary = spendReportingResults.spendSummary
    spendReportingResults.spendDetails.flatMap { spendDetail =>
      spendDetail.spendData.map { spendData =>
        val spendCategoryMap: Map[Option[TerraSpendCategories.TerraSpendCategory], SpendReportingForDateRange] =
          spendData.subAggregation
            .flatMap(_.spendData)
            .map(datedReport => datedReport.category -> datedReport)
            .toMap

        // helper to look up the cost and credits for a given category
        def getCategoryCost(category: TerraSpendCategories.TerraSpendCategory): (Option[Float], Option[Float]) =
          spendCategoryMap
            .get(Some(category))
            .map { categorySpend =>
              (Option(categorySpend.cost.toFloat), Option(categorySpend.credits.toFloat))
            }
            .getOrElse((None, None))

        // get cost and credits for the categories we care about
        val (totalStorage, storageCredits) = getCategoryCost(TerraSpendCategories.Storage)
        val (totalCompute, computeCredits) = getCategoryCost(TerraSpendCategories.Compute)
        val (otherSpend, otherCredits) = getCategoryCost(TerraSpendCategories.Other)

        WorkspaceSpendReport.newWorkspaceSpendReport(
          spendData.googleProjectId.get.value,
          SpendReportUtils.convertJodaToJava(summary.startTime),
          SpendReportUtils.convertJodaToJava(summary.endTime),
          summary.currency,
          isDataAvailable = true,
          totalCompute,
          totalStorage,
          otherSpend,
          computeCredits,
          storageCredits,
          otherCredits
        )
      }
    }
  }

  def toSpendReportingResults(records: Seq[WorkspaceSpendReport],
                              projectNames: Map[GoogleProjectId, WorkspaceName]
  ): SpendReportingResults = {
    var start: Option[DateTime] = None
    var end: Option[DateTime] = None
    var total_spend = BigDecimal(0.0)
    var total_credits = BigDecimal(0.0)
    val currency: Currency = SpendReportUtils.getCurrency(records.map(_.currency))
    val all = records.map { record =>
      val currencyString = record.currency
      val currencyCode = Currency.getInstance(currencyString)
      val projectId = record.googleProjectId

      val subAggregation = List(
        SpendReportingForDateRange(
          SpendReportUtils.toBigDecimal(record.otherSpend, currencyCode).toString(),
          SpendReportUtils.toBigDecimal(record.otherCredits, currencyCode).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Other)
        ),
        SpendReportingForDateRange(
          SpendReportUtils.toBigDecimal(record.totalStorage, currencyCode).toString(),
          SpendReportUtils.toBigDecimal(record.storageCredits, currencyCode).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Storage)
        ),
        SpendReportingForDateRange(
          SpendReportUtils.toBigDecimal(record.totalCompute, currencyCode).toString(),
          SpendReportUtils.toBigDecimal(record.computeCredits, currencyCode).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Compute)
        )
      )

      val cost: Option[Float] = Option(Seq(record.totalCompute, record.totalStorage, record.otherSpend).flatten.sum)
      val credits: Option[Float] =
        Option(Seq(record.computeCredits, record.storageCredits, record.otherCredits).flatten.sum)
      total_spend = total_spend + SpendReportUtils.toBigDecimal(cost, currencyCode)
      total_credits = total_credits + SpendReportUtils.toBigDecimal(credits, currencyCode)
      start = SpendReportUtils.convertLocalDateTimeToJodaDateTime(record.reportStartDate)
      end = SpendReportUtils.convertLocalDateTimeToJodaDateTime(record.reportEndDate)
      val workspaceTotal = SpendReportingForDateRange(
        total_spend.toString,
        total_credits.toString,
        currency.toString,
        start,
        end,
        workspace = projectNames.get(GoogleProjectId(projectId)),
        googleProjectId = Option(GoogleProject(projectId)),
        subAggregation = Option(SpendReportingAggregation(SpendReportingAggregationKeys.Category, subAggregation))
      )
      SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, List(workspaceTotal))
    }

    val summary = SpendReportingForDateRange(
      total_spend.toString,
      total_credits.toString,
      currency.toString,
      start,
      end
    )
    SpendReportingResults(all, summary)
  }

}

trait WorkspaceSpendReportComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceSpendReportTable(tag: Tag) extends Table[WorkspaceSpendReportRecord](tag, "WORKSPACE_SPEND_REPORT") {

    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)

    def googleProjectId = column[String]("GOOGLE_PROJECT_ID", O.Length(254))

    def reportStartDate = column[Timestamp]("REPORT_START_DATE", O.SqlType("DATETIME"))

    def reportEndDate = column[Timestamp]("REPORT_END_DATE", O.SqlType("DATETIME"))

    def currency = column[String]("CURRENCY", O.Length(100))

    def isDataAvailable = column[Boolean]("IS_DATA_AVAILABLE")

    def totalCompute = column[Option[Float]]("TOTAL_COMPUTE", O.SqlType("NUMBER(10,2)"))

    def totalStorage = column[Option[Float]]("TOTAL_STORAGE", O.SqlType("NUMBER(10,2)"))

    def otherSpend = column[Option[Float]]("OTHER_SPEND", O.SqlType("NUMBER(10,2)"))

    def computeCredits = column[Option[Float]]("COMPUTE_CREDITS", O.SqlType("NUMBER(10,2)"))

    def storageCredits = column[Option[Float]]("STORAGE_CREDITS", O.SqlType("NUMBER(10,2)"))

    def otherCredits = column[Option[Float]]("OTHER_CREDITS", O.SqlType("NUMBER(10,2)"))

    def * = (id,
             googleProjectId,
             reportStartDate,
             reportEndDate,
             currency,
             isDataAvailable,
             totalCompute,
             totalStorage,
             otherSpend,
             computeCredits,
             storageCredits,
             otherCredits
    ) <> ((WorkspaceSpendReportRecord.apply _).tupled, WorkspaceSpendReportRecord.unapply)

  }

  protected val workspaceSpendReportQuery = TableQuery[WorkspaceSpendReportTable]

  type WorkspaceSpendReportQueryType = driver.api.Query[WorkspaceSpendReportTable, WorkspaceSpendReportRecord, Seq]

  object WorkspaceSpendReportQuery extends TableQuery(new WorkspaceSpendReportTable(_)) {

    def getWorkspaceSpendReportByProjectIdsAndReportDate(projectIds: Set[String],
                                                         startDate: LocalDateTime,
                                                         endDate: LocalDateTime
    ): ReadAction[Seq[WorkspaceSpendReport]] =
      loadWorkspaceSpendReport(filterByProjectIdsAndReportDate(projectIds, startDate, endDate))

    def filterByProjectIdsAndReportDate(projectIds: Set[String],
                                        startDate: LocalDateTime,
                                        endDate: LocalDateTime
    ): Query[WorkspaceSpendReportTable, WorkspaceSpendReportRecord, Seq] =
      workspaceSpendReportQuery
        .filter(x =>
          x.googleProjectId.inSetBind(
            projectIds.map(_.value)
          ) && x.reportStartDate === new Timestamp(startDate.toInstant(ZoneOffset.UTC).toEpochMilli)
            && x.reportEndDate === new Timestamp(endDate.toInstant(ZoneOffset.UTC).toEpochMilli)
        )

    private def loadWorkspaceSpendReport(lookup: WorkspaceSpendReportQueryType): ReadAction[Seq[WorkspaceSpendReport]] =
      for {
        records <- lookup.result
      } yield records.map { record =>
        WorkspaceSpendReportRecord.toWorkspaceSpendReport(record)
      }

    def insert(workspaceSpendReport: WorkspaceSpendReport): WriteAction[Long] =
      workspaceSpendReportQuery returning workspaceSpendReportQuery.map(_.id) += WorkspaceSpendReportRecord
        .fromWorkspaceSpendReport(workspaceSpendReport)
  }

}
