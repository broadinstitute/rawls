package org.broadinstitute.dsde.rawls.dataaccess.slick

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
        spendData =>
          spendData.subAggregation.get.spendData.foreach { categorySpend =>
            val categoryCost = Some(categorySpend.cost.toFloat)
            categorySpend.category match {
              case Some(TerraSpendCategories.Storage) => totalStorage = categoryCost
              case Some(TerraSpendCategories.Compute) => totalCompute = categoryCost
              case Some(TerraSpendCategories.Other)   => otherSpend = categoryCost
              case None => throw new IllegalArgumentException("Spend data has no category") // Does this ever happen?
            }
          }
          // TODO: Store credits and currency (what about aggregation key)?
          WorkspaceSpendReport.newWorkspaceSpendReport(
            spendData.googleProjectId.get.value,
            convertJodaToJava(summary.startTime),
            convertJodaToJava(summary.endTime),
            totalCompute,
            totalStorage,
            otherSpend,
            isDataAvailable = true
          ) // TODO: Handle N/A case
      }
    }
  }

  def toSpendReportingResults(records: Seq[WorkspaceSpendReport],
                              projectNames: Map[GoogleProjectId, WorkspaceName]
  ): SpendReportingResults = {
    var start: Option[DateTime] = None
    var end: Option[DateTime] = None
    var total = BigDecimal(0.0)
    var total_credits = BigDecimal(0.0)
    val all = records.map { record =>
      val currencyString = "$"
      val currencyCode = Currency.getInstance(currencyString)
      val projectId = record.googleProjectId

      def toBigDecimal(cost: Option[Float]): BigDecimal =
        BigDecimal(cost.getOrElse(0.0f)).setScale(currencyCode.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

      val credits = BigDecimal(0.0).toString() // TODO: Store credits per category
      val subAggregation = List(
        SpendReportingForDateRange(
          toBigDecimal(record.otherSpend).toString(),
          credits,
          currencyCode.toString,
          category = Option(TerraSpendCategories.Other)
        ),
        SpendReportingForDateRange(
          toBigDecimal(record.totalStorage).toString(),
          credits,
          currencyCode.toString,
          category = Option(TerraSpendCategories.Storage)
        ),
        SpendReportingForDateRange(
          toBigDecimal(record.totalCompute).toString(),
          credits,
          currencyCode.toString,
          category = Option(TerraSpendCategories.Compute)
        )
      )

      val totalCompute: Option[Float] = record.totalCompute
      val totalStorage: Option[Float] = record.totalStorage
      val otherSpend: Option[Float] = record.otherSpend
      val total_cost: Option[Float] = for {
        c1 <- totalCompute
        c2 <- totalStorage
        c3 <- otherSpend
      } yield c1 + c2 + c3

//      val credits =
//        getRoundedNumericValue("other_credits") + getRoundedNumericValue("storage_credits") + getRoundedNumericValue(
//          "compute_credits"
//        )
      total = total + toBigDecimal(total_cost)
//      total_credits = total_credits + credits
      start = convertLocalDateTimeToJodaDateTime(record.reportStartDate)
      end = convertLocalDateTimeToJodaDateTime(record.reportEndDate)
      val workspaceTotal = SpendReportingForDateRange(
        total_cost.toString,
        credits,
        currencyCode.toString,
        start,
        end,
        workspace = projectNames.get(GoogleProjectId(projectId)),
        googleProjectId = Option(GoogleProject(projectId)),
        subAggregation = Option(SpendReportingAggregation(SpendReportingAggregationKeys.Category, subAggregation))
      )
      SpendReportingAggregation(SpendReportingAggregationKeys.Workspace, List(workspaceTotal))
    }

    val summary = SpendReportingForDateRange(
      total.toString,
      total_credits.toString,
      "USD",
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
             isDataAvailable
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
