package org.broadinstitute.dsde.rawls.spendreporting

import java.util
import java.util.Currency

import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SpendReportingService = {
    new SpendReportingService(userInfo, dataSource, bigQueryDAO, samDAO)
  }
}


class SpendReportingService(userInfo: UserInfo, dataSource: SlickDataSource, bigQueryDAO: GoogleBigQueryDAO, samDAO: SamDAO)
                           (implicit val executionContext: ExecutionContext) extends LazyLogging {

  def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(op: => Future[T]): Future[T] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")))
    }
  }

  def extractSpendReportingResults(rows: util.List[TableRow], startTime: DateTime, endTime: DateTime): SpendReportingResults = {
    val currency = Currency.getInstance(rows.asScala.head.getF.get(2).getV.toString)

    val dailySpend = rows.asScala.map { row =>
      val rowCost = BigDecimal(row.getF.get(0).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowCredits = BigDecimal(row.getF.get(1).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      SpendReportingForDateRange(rowCost.toString(),
        rowCredits.toString(),
        currency.getCurrencyCode,
        DateTime.parse(row.getF.get(3).getV.toString),
        DateTime.parse(row.getF.get(3).getV.toString).plusDays(1).minusSeconds(1))
    }
    val dailySpendAggregation = SpendReportingAggregation(
      SpendReportingAggregationKey(""), dailySpend
    )

    val costRollup = rows.asScala.map { row =>
      BigDecimal(row.getF.get(0).getV.toString)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    val creditsRollup = rows.asScala.map { row =>
      BigDecimal(row.getF.get(1).getV.toString)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

    val spendSummary = SpendReportingForDateRange(
      costRollup.toString(),
      creditsRollup.toString(),
      currency.getCurrencyCode,
      startTime,
      endTime
    )

    SpendReportingResults(Seq(dailySpendAggregation), spendSummary)
  }

  def dateTimeToISODateString(dt: DateTime) =
    dt.toString(ISODateTimeFormat.date())

  // todo: naming?
  private def getSpendReportConfiguration(project: Option[RawlsBillingProject]): (RawlsBillingAccountName, GoogleProject, BigQueryDatasetName, BigQueryTableName) = {
    project match {
      case Some(RawlsBillingProject(_, _, Some(billingAccount), _, _, _, _, _, Some(spendReportDataset), Some(spendReportTable), Some(spendReportDatasetGoogleProject))) => (billingAccount, spendReportDatasetGoogleProject, spendReportDataset, spendReportTable)
      case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"project not found"))
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"spend report configuration not set on billing project"))
    }
  }

  private def validateReportParameters(startDate: DateTime, endDate: DateTime): Unit = {
    if (startDate.isAfter(endDate)) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "start date must be before end date"))
  }

  def getSpendForBillingProject(billingProject: RawlsBillingProjectName, startDate: DateTime, endDate: DateTime): Future[SpendReportingResults] = {
    validateReportParameters(startDate, endDate)
    requireProjectAction(billingProject, SamBillingProjectActions.alterSpendReportConfiguration) { // todo: new action here? this is an okay approx. but could add a specific one
      for {
        projectOpt <- dataSource.inTransaction { dataAccess => // todo: maybe this should get pulled into getSpendReportConfiguration too
          dataAccess.rawlsBillingProjectQuery.load(billingProject)
        }

        (billingAccountId, spendReportingGoogleProject, spendReportingDataset, spendReportingTableName) = getSpendReportConfiguration(projectOpt)

        // todo: by billing account or by project?
        query =
        s"""
           | SELECT
           |  SUM(cost) as cost,
           |  SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) as credits,
           |  currency,
           |  DATE(_PARTITIONTIME) as date
           | FROM `${spendReportingGoogleProject}.${spendReportingDataset}.${spendReportingTableName}`
           | WHERE billing_account_id = @billingAccountId
           | AND _PARTITIONDATE BETWEEN @startDate AND @endDate
           | GROUP BY currency, date
           |""".stripMargin

        queryParams = List(
          new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("billingAccountId").setParameterValue(new QueryParameterValue().setValue(billingAccountId.value)),
          new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("startDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(startDate))),
          new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("endDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(endDate))),
        )

        jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject(spendReportingGoogleProject.value), query, queryParams, "NAMED")
        jobStatus <- bigQueryDAO.getQueryStatus(jobRef)
        result: GetQueryResultsResponse <- bigQueryDAO.getQueryResult(jobStatus)
      } yield {
        extractSpendReportingResults(result.getRows, startDate, endDate)
      }
    }
  }
}
