package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime

import java.util.Currency
import scala.language.{postfixOps, reflectiveCalls}
import scala.math.BigDecimal.RoundingMode

case class WorkspaceSpendReportRecord(
  id: Long,
  googleProjectId: String,
  reportStartDate: LocalDateTime,
  reportEndDate: LocalDateTime,
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
      workspaceSpendReport.reportStartDate,
      workspaceSpendReport.reportEndDate,
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
      record.reportStartDate,
      record.reportEndDate,
      record.currency,
      record.isDataAvailable,
      record.totalCompute,
      record.totalStorage,
      record.otherSpend,
      record.computeCredits,
      record.storageCredits,
      record.otherCredits
    )

  def convertJodaToJava(dateTimeOpt: Option[DateTime]): LocalDateTime =
    dateTimeOpt match {
      case Some(dateTime) =>
        val instant = dateTime.toInstant
        LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(dateTime.getMillis), ZoneId.systemDefault())
      case None =>
        throw new IllegalArgumentException("DateTime value is missing")
    }

  def convertLocalDateTimeToJodaDateTime(localDateTime: LocalDateTime,
                                         zoneId: ZoneId = ZoneId.systemDefault()
  ): Option[DateTime] =
    Option(localDateTime).map { ldt =>
      // Convert LocalDateTime to ZonedDateTime
      val zonedDateTime: ZonedDateTime = ldt.atZone(zoneId)

      // Convert ZonedDateTime to Joda DateTime using epoch milli
      new DateTime(zonedDateTime.toInstant.toEpochMilli)
    }

  def fromSpendReportingResults(spendReportingResults: SpendReportingResults): Seq[WorkspaceSpendReport] = {
    val summary = spendReportingResults.spendSummary
    spendReportingResults.spendDetails.flatMap { spendDetail =>
      spendDetail.spendData.map {
        var totalStorage: Option[Float] = None
        var totalCompute: Option[Float] = None
        var otherSpend: Option[Float] = None
        var storageCredits: Option[Float] = None
        var computeCredits: Option[Float] = None
        var otherCredits: Option[Float] = None
        spendData =>
          spendData.subAggregation.get.spendData.foreach { categorySpend =>
            val categoryCost = Some(categorySpend.cost.toFloat)
            val categoryCredits = Some(categorySpend.credits.toFloat)
            categorySpend.category match {
              case Some(TerraSpendCategories.Storage) =>
                totalStorage = categoryCost
                storageCredits = categoryCredits
              case Some(TerraSpendCategories.Compute) =>
                totalCompute = categoryCost
                computeCredits = categoryCredits
              case Some(TerraSpendCategories.Other) =>
                otherSpend = categoryCost
                otherCredits = categoryCredits
            }
          }
          WorkspaceSpendReport.newWorkspaceSpendReport(
            spendData.googleProjectId.get.value,
            convertJodaToJava(summary.startTime),
            convertJodaToJava(summary.endTime),
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
    val currency = records.map(_.currency).distinct match {
      case head :: List() => Currency.getInstance(head)
      case head :: tail =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.InternalServerError,
          s"Inconsistent currencies found while aggregating spend data: $head and ${tail.head} cannot be combined"
        )
      case List() => throw RawlsExceptionWithErrorReport(StatusCodes.NotFound, "No currencies found for spend data")
    }

    val all = records.map { record =>
      val currencyString = record.currency
      val currencyCode = Currency.getInstance(currencyString)
      val projectId = record.googleProjectId

      def toBigDecimal(cost: Option[Float]): BigDecimal =
        BigDecimal(cost.getOrElse(0.0f)).setScale(currencyCode.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

      val subAggregation = List(
        SpendReportingForDateRange(
          toBigDecimal(record.otherSpend).toString(),
          toBigDecimal(record.otherCredits).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Other)
        ),
        SpendReportingForDateRange(
          toBigDecimal(record.totalStorage).toString(),
          toBigDecimal(record.storageCredits).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Storage)
        ),
        SpendReportingForDateRange(
          toBigDecimal(record.totalCompute).toString(),
          toBigDecimal(record.computeCredits).toString(),
          currencyCode.toString,
          category = Option(TerraSpendCategories.Compute)
        )
      )

      val totalCompute: Option[Float] = record.totalCompute
      val totalStorage: Option[Float] = record.totalStorage
      val otherSpend: Option[Float] = record.otherSpend
      val cost: Option[Float] = for {
        cost1 <- totalCompute
        cost2 <- totalStorage
        cost3 <- otherSpend
      } yield cost1 + cost2 + cost3

      val computeCredits: Option[Float] = record.computeCredits
      val storageCredits: Option[Float] = record.storageCredits
      val otherCredits: Option[Float] = record.otherCredits
      val credits: Option[Float] = for {
        credit1 <- computeCredits
        credit2 <- storageCredits
        credit3 <- otherCredits
      } yield credit1 + credit2 + credit3

      total_spend = total_spend + toBigDecimal(cost)
      total_credits = total_credits + toBigDecimal(credits)
      start = convertLocalDateTimeToJodaDateTime(record.reportStartDate)
      end = convertLocalDateTimeToJodaDateTime(record.reportEndDate)
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

    def reportStartDate = column[LocalDateTime]("REPORT_START_DATE", O.SqlType("DATETIME"))

    def reportEndDate = column[LocalDateTime]("REPORT_END_DATE", O.SqlType("DATETIME"))

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

    def filterByProjectIdsAndReportDate(projectIds: Set[String], startDate: LocalDateTime, endDate: LocalDateTime) =
      workspaceSpendReportQuery
        .filter(x =>
          x.googleProjectId.inSetBind(
            projectIds.map(_.value)
          ) && x.reportStartDate === startDate && x.reportEndDate === endDate
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
