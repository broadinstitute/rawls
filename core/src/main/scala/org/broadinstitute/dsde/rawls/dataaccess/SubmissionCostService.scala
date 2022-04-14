package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.util
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object SubmissionCostService {
  def constructor(defaultTableName: String, serviceProject: String, billingSearchWindowDays: Int, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) =
    new SubmissionCostService(defaultTableName, serviceProject, billingSearchWindowDays, bigQueryDAO)
}

class SubmissionCostService(defaultTableName: String, serviceProject: String, billingSearchWindowDays: Int, bigQueryDAO: GoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  val stringParamType = new QueryParameterType().setType("STRING")


  def getSubmissionCosts(submissionId: String, workflowIds: Seq[String], googleProjectId: GoogleProjectId, submissionDate: DateTime, terminalStatusDate: Option[DateTime], tableNameOpt: Option[String] = Option(defaultTableName)): Future[Map[String, Float]] = {
    val tableName = tableNameOpt.getOrElse(defaultTableName)

    if( workflowIds.isEmpty ) {
      Future.successful(Map.empty[String, Float])
    } else {
      for {
        //try looking up the workflows via the submission ID.
        //this makes for a smaller query string (though no faster).
        submissionCosts <- executeSubmissionCostsQuery(submissionId, googleProjectId, submissionDate, terminalStatusDate, tableName)
        //if that doesn't return anything, fall back to
        fallbackCosts <- if (submissionCosts.size() == 0)
          executeWorkflowCostsQuery(workflowIds, googleProjectId, submissionDate, terminalStatusDate, tableName)
        else
          Future.successful(submissionCosts)
      } yield {
        extractCostResults(fallbackCosts)
      }
    }
  }


  def getWorkflowCost(workflowId: String,
                      googleProjectId: GoogleProjectId,
                      submissionDate: DateTime,
                      terminalStatusDate: Option[DateTime],
                      tableNameOpt: Option[String] = Option(defaultTableName)): Future[Map[String, Float]] = {
    val tableName = tableNameOpt.getOrElse(defaultTableName)

    executeWorkflowCostsQuery(Seq(workflowId), googleProjectId, submissionDate, terminalStatusDate, tableName) map extractCostResults
  }

  /*
   * Manipulates and massages a BigQuery result.
   */
  def extractCostResults(rows: util.List[TableRow]): Map[String, Float] = {
    Option(rows) match {
      case Some(rows) => rows.asScala.map { row =>
        // workflow ID is contained in the 2nd cell, cost is contained in the 3rd cell
        row.getF.get(1).getV.toString -> row.getF.get(2).getV.toString.toFloat
      }.toMap
      case None => Map.empty[String, Float]
    }
  }

  private def partitionDateClause(submissionDate: DateTime, terminalStatusDate: Option[DateTime]): String = {
    // subtract a day so we never have to deal with timezones
    val windowStartDate = submissionDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    val windowEndDate = terminalStatusDate
      // if this submission has no date at which it reached terminal state, use the default window from config
      .getOrElse(submissionDate.plusDays(billingSearchWindowDays))
      // add a day so we never have to deal with timezones
      .plusDays(1)
      .toString(DateTimeFormat.forPattern("yyyy-MM-dd"))

    s"""AND _PARTITIONDATE BETWEEN "$windowStartDate" AND "$windowEndDate""""
  }

  private def executeSubmissionCostsQuery(submissionId: String,
                                          googleProjectId: GoogleProjectId,
                                          submissionDate: DateTime,
                                          terminalStatusDate: Option[DateTime],
                                          tableName: String): Future[util.List[TableRow]] = {

    val querySql: String =
      generateSubmissionCostsQuery(submissionId, submissionDate, terminalStatusDate, tableName)

    val namespaceParam =
      new QueryParameter()
        .setParameterType(stringParamType)
        .setParameterValue(new QueryParameterValue().setValue(googleProjectId.value))

    val queryParameters: List[QueryParameter] = List(namespaceParam)

    executeBigQuery(querySql, queryParameters) map { result =>
      val rowsReturned =  Option(result.getTotalRows).getOrElse(0)
      val bytesProcessed = Option(result.getTotalBytesProcessed).getOrElse(0)
      logger.debug(s"Queried for costs of submission $submissionId: $rowsReturned Rows Returned and $bytesProcessed Bytes Processed.")
      Option(result.getRows).getOrElse(List.empty[TableRow].asJava)
    }
  }

  /*
   * Queries BigQuery for compute costs associated with the workflowIds.
   */
  private def executeWorkflowCostsQuery(workflowIds: Seq[String],
                                        googleProjectId: GoogleProjectId,
                                        submissionDate: DateTime,
                                        terminalStatusDate: Option[DateTime],
                                        tableName: String): Future[util.List[TableRow]] = {
    workflowIds match {
      case Seq() => Future.successful(Seq.empty.asJava)
      case ids =>
        val subquery = ids.map(_ => s"""workflowId LIKE ?""").mkString(" OR ")
        val querySql: String =
          generateWorkflowCostsQuery(submissionDate, terminalStatusDate, subquery, tableName)

        val namespaceParam =
          new QueryParameter()
            .setParameterType(stringParamType)
            .setParameterValue(new QueryParameterValue().setValue(googleProjectId.value))
        val subqueryParams = workflowIds.toList map { workflowId =>
          new QueryParameter()
            .setParameterType(stringParamType)
            .setParameterValue(new QueryParameterValue().setValue(s"%$workflowId%"))
        }
        val queryParameters: List[QueryParameter] = namespaceParam :: subqueryParams

        executeBigQuery(querySql, queryParameters) map { result =>
          val idCount = ids.length
          val rowsReturned = Option(result.getTotalRows).getOrElse(0)
          val bytesProcessed = Option(result.getTotalBytesProcessed).getOrElse(0)
          logger.debug(s"Queried for costs of $idCount Workflow IDs: $rowsReturned Rows Returned and $bytesProcessed Bytes Processed.")
          Option(result.getRows).getOrElse(List.empty[TableRow].asJava)
        }
    }
  }

  def generateSubmissionCostsQuery(submissionId: String, submissionDate: DateTime, terminalStatusDate: Option[DateTime], tableName: String): String = {
    s"""SELECT wflabels.key, REPLACE(wflabels.value, "cromwell-", "") as `workflowId`, SUM(billing.cost)
       |FROM `$tableName` as billing, UNNEST(labels) as wflabels
       |CROSS JOIN UNNEST(billing.labels) as blabels
       |WHERE blabels.value = "terra-$submissionId"
       |AND wflabels.key = "cromwell-workflow-id"
       |AND project.id = ?
       |${partitionDateClause(submissionDate, terminalStatusDate)}
       |GROUP BY wflabels.key, workflowId""".stripMargin
  }

  def generateWorkflowCostsQuery(submissionDate: DateTime, terminalStatusDate: Option[DateTime], subquery: String, tableName: String): String = {
    s"""|SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
        |FROM `$tableName`, UNNEST(labels) as labels
        |WHERE project.id = ?
        |AND labels.key LIKE "cromwell-workflow-id"
        |${partitionDateClause(submissionDate, terminalStatusDate)}
        |GROUP BY labels.key, workflowId
        |HAVING $subquery""".stripMargin
  }

  private def executeBigQuery(querySql: String, queryParams: List[QueryParameter]): Future[GetQueryResultsResponse] = {
    for {
      jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject(serviceProject), querySql, queryParams, "POSITIONAL")
      job <- bigQueryDAO.getQueryStatus(jobRef)
      result <- bigQueryDAO.getQueryResult(job)
    } yield {
      result
    }
  }
}
