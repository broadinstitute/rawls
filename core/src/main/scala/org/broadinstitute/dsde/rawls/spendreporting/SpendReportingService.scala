package org.broadinstitute.dsde.rawls.spendreporting

import com.google.api.services.bigquery.model.{GetQueryResultsResponse, QueryParameter, QueryParameterType, QueryParameterValue, TableRow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, JsonSupport, SpendReportingResults, UserInfo}
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat2}

import java.util
import scala.concurrent.{ExecutionContext, Future}

object SpendReportingService {
  def constructor(bigQueryDAO: GoogleBigQueryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) = {
    new SpendReportingService( bigQueryDAO)
  }
}


class SpendReportingService(  bigQueryDAO: GoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def extractSpendReportingResults(rows: util.List[TableRow]): SpendReportingResults = {
    SpendReportingResults("hello world!", "123.45")
  }

  def dateTimeToISODateString(dt: DateTime) =
    dt.toString(ISODateTimeFormat.date())

  def getSpendForBillingAccount(spendReportingGoogleProject: GoogleProjectId, spendReportingDataset: String, spendReportingTableName: String,  billingAccountId: String, startDate: DateTime, endDate: DateTime): Future[SpendReportingResults] = {
    val query =
      s"""
         | SELECT SUM(cost)
         | FROM `${spendReportingGoogleProject}.${spendReportingDataset}.${spendReportingTableName}`
         | WHERE billing_account_id = @billingAccountId
         | AND PARTITIONDATE BETWEEN @startDate AND @endDate
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
      extractSpendReportingResults(result.getRows)
    }
  }

}