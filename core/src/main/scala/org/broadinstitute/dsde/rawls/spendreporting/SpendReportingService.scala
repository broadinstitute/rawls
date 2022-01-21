package org.broadinstitute.dsde.rawls.spendreporting

import com.google.api.services.bigquery.model.{GetQueryResultsResponse, QueryParameter, QueryParameterType, QueryParameterValue, TableRow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, JsonSupport, SpendReportingAggregation, SpendReportingAggregationKey, SpendReportingForDateRange, SpendReportingResults, UserInfo}
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat2}

import scala.jdk.CollectionConverters._
import java.util
import java.util.Currency
import scala.concurrent.{ExecutionContext, Future}
import scala.math.BigDecimal.RoundingMode

object SpendReportingService {
  def constructor(bigQueryDAO: GoogleBigQueryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) = {
    new SpendReportingService( bigQueryDAO)
  }
}


class SpendReportingService(  bigQueryDAO: GoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def extractSpendReportingResults(rows: util.List[TableRow], startTime: DateTime, endTime: DateTime): SpendReportingResults = {
    val currency = Currency.getInstance(rows.asScala.head.getF.get(2).getV.toString)

    val perDateSpend = rows.asScala.map { row =>
      val rowCost = BigDecimal(row.getF.get(0).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      val rowCredits = BigDecimal(row.getF.get(1).getV.toString).setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
      SpendReportingForDateRange(rowCost.toString(),
        rowCredits.toString(),
        currency.getCurrencyCode,
        DateTime.parse(row.getF.get(3).getV.toString),
        DateTime.parse(row.getF.get(3).getV.toString).plusDays(1).minusSeconds(1))
    }
    val spendDetails = SpendReportingAggregation(
      SpendReportingAggregationKey(""), perDateSpend
    )
    val cost = rows.asScala.map { row =>
      BigDecimal(row.getF.get(0).getV.toString)//
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)
    val credits = rows.asScala.map { row =>
      BigDecimal(row.getF.get(1).getV.toString)
    }.sum.setScale(currency.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

    val spendSummary = SpendReportingForDateRange(
      cost.toString(),
      credits.toString(),
      currency.getCurrencyCode,
      startTime,
      endTime
    )

    SpendReportingResults(Seq(spendDetails), spendSummary)
  }

  def dateTimeToISODateString(dt: DateTime) =
    dt.toString(ISODateTimeFormat.date())

  def getSpendForBillingAccount(spendReportingGoogleProject: GoogleProjectId, spendReportingDataset: String, spendReportingTableName: String,  billingAccountId: String, startDate: DateTime, endDate: DateTime): Future[SpendReportingResults] = {
    val query =
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

    val queryParams = List(
      new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("billingAccountId").setParameterValue(new QueryParameterValue().setValue(billingAccountId)),
      new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("startDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(startDate))),
      new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setName("endDate").setParameterValue(new QueryParameterValue().setValue(dateTimeToISODateString(endDate))),
    )
    for {
      jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject(spendReportingGoogleProject.value), query, queryParams, "NAMED")
      jobStatus <- bigQueryDAO.getQueryStatus(jobRef)
      result: GetQueryResultsResponse <- bigQueryDAO.getQueryResult(jobStatus)
    } yield {
      extractSpendReportingResults(result.getRows, startDate, endDate)
    }
  }
}