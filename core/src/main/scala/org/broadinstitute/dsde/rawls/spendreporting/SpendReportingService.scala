package org.broadinstitute.dsde.rawls.spendreporting

import com.google.api.services.bigquery.model.{GetQueryResultsResponse, QueryParameter, QueryParameterType, QueryParameterValue, TableRow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, JsonSupport, SpendReportingResults, UserInfo}
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat2}

import java.util
import scala.concurrent.{ExecutionContext, Future}

object SpendReportingService {
  def constructor(tableName: String, billingProjectId: GoogleProjectId, billingTableName: String, bigQueryDAO: GoogleBigQueryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) = {
    new SpendReportingService(tableName, billingProjectId, billingTableName, bigQueryDAO)
  }
}



class SpendReportingService(tableName: String, billingProjectId: GoogleProjectId, billingTableName: String, bigQueryDAO: GoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def extractSpendReportingResults(rows: util.List[TableRow]): SpendReportingResults = {
    SpendReportingResults("hello world!", "123.45")
  }

  def getSpendForBillingAccount(billingAccount: String): Future[SpendReportingResults] = {
    val query =
      s"""
         | SELECT SUM(cost)
         | FROM `molten-sandbox-318221.daily_cost_detail.gcp_billing_export_v1_01B1BE_776EE8_991558`
         | WHERE billing_account_id = ?
         | AND PARTITIONDATE BETWEEN '2021-12-01' AND '2021-01-01'
         |""".stripMargin

    val queryParams = List(new QueryParameter().setParameterType(new QueryParameterType().setType("STRING")).setParameterValue(new QueryParameterValue().setValue(billingAccount)))
    for {
      jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject("molten-sandbox-318221"), query, queryParams, "POSITIONAL")
      jobStatus <- bigQueryDAO.getQueryStatus(jobRef)
      result: GetQueryResultsResponse <- bigQueryDAO.getQueryResult(jobStatus)
    } yield {
      extractSpendReportingResults(result.getRows)
    }


  }

}