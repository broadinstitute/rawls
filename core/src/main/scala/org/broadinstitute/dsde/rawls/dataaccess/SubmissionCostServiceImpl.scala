package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowActualCostRecord
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId}
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.util
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

object SubmissionCostServiceImpl {
  def constructor(defaultTableName: String,
                  defaultDatePartitionColumn: String,
                  serviceProject: String,
                  billingSearchWindowDays: Int,
                  dataSource: SlickDataSource,
                  bigQueryDAO: GoogleBigQueryDAO
  )(implicit executionContext: ExecutionContext) =
    new SubmissionCostServiceImpl(defaultTableName,
                                  defaultDatePartitionColumn,
                                  serviceProject,
                                  billingSearchWindowDays,
                                  dataSource,
                                  bigQueryDAO
    )
}

class SubmissionCostServiceImpl(defaultTableName: String,
                                defaultDatePartitionColumn: String,
                                serviceProject: String,
                                billingSearchWindowDays: Int,
                                dataSource: SlickDataSource,
                                bigQueryDAO: GoogleBigQueryDAO
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging
    with SubmissionCostService {

  val stringParamType: QueryParameterType = new QueryParameterType().setType("STRING")

  /**
   * Retrieve actual costs for multiple workflows.
   */
  def getSubmissionCosts(submissionIdStr: String,
                         workflowIds: Seq[String],
                         googleProjectId: GoogleProjectId,
                         submissionDate: DateTime,
                         terminalStatusDate: Option[DateTime],
                         tableNameOpt: Option[String] = Option(defaultTableName)
  ): Future[Map[String, Float]] =
    if (workflowIds.isEmpty) {
      Future.successful(Map.empty[String, Float])
    } else {
      val submissionId = Try(UUID.fromString(submissionIdStr)).getOrElse(
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, "invalid submission id; must be a UUID.")
        )
      )
      for {
        // ask WORKFLOW_ACTUAL_COST table for any cached costs
        cachedResults: Seq[WorkflowActualCostRecord] <- retrieveCostsFromLocalDb(workflowIds)
        // determine which of the requested workflows were not found in WORKFLOW_ACTUAL_COST
        uncachedWorkflows = workflowIds.toSet diff cachedResults.map(_.externalId).toSet
        // ask BigQuery for any workflows which weren't found in WORKFLOW_ACTUAL_COST
        liveResults <-
          if (uncachedWorkflows.nonEmpty) {
            logger.info(s"getSubmissionCosts: ${uncachedWorkflows.size} workflows not found in cache; asking BigQuery")
            retrieveCostsFromBigQuery(uncachedWorkflows.toSeq,
                                      googleProjectId,
                                      submissionDate,
                                      terminalStatusDate,
                                      tableNameOpt
            )
          } else {
            logger.info(s"getSubmissionCosts: all workflows found in cache; bypassing BigQuery")
            Future.successful(Map.empty[String, Float])
          }
        // determine which of the uncachedWorkflows did not have a hit in BigQuery
        notFoundWorkflows = uncachedWorkflows diff liveResults.keySet
        // persist BigQuery results back to WORKFLOW_ACTUAL_COST
        _ <-
          if (liveResults.nonEmpty || notFoundWorkflows.nonEmpty)
            writeCostsToLocalDb(submissionId, liveResults, notFoundWorkflows)
          else
            Future.successful(-1)
      } yield {
        // extract from the cached results only those workflows which actually have a cost
        val cachedResultsWithCost = cachedResults.collect {
          case r: WorkflowActualCostRecord if r.cost.isDefined =>
            (r.externalId, r.cost.get)
        }.toMap
        // return the union of WORKFLOW_ACTUAL_COST and BigQuery results
        cachedResultsWithCost ++ liveResults
      }
    }

  /**
   * Retrieve the actual cost for a single workflow.
   */
  def getWorkflowCost(submissionId: String,
                      workflowId: String,
                      googleProjectId: GoogleProjectId,
                      submissionDate: DateTime,
                      terminalStatusDate: Option[DateTime],
                      tableNameOpt: Option[String] = Option(defaultTableName)
  ): Future[Map[String, Float]] =
    getSubmissionCosts(submissionId, Seq(workflowId), googleProjectId, submissionDate, terminalStatusDate, tableNameOpt)

  // ask WORKFLOW_ACTUAL_COST table for specific workflows
  private def retrieveCostsFromLocalDb(
    workflowIds: Seq[String]
  ): Future[Seq[WorkflowActualCostRecord]] = {
    import dataSource.dataAccess.driver.api._
    dataSource
      .inTransaction { dataAccess =>
        dataAccess.workflowActualCostQuery.filter(row => row.externalId.inSetBind(workflowIds)).result
      }
  }

  // modular method ask BigQuery for specific workflows
  private def retrieveCostsFromBigQuery(workflowIds: Seq[String],
                                        googleProjectId: GoogleProjectId,
                                        submissionDate: DateTime,
                                        terminalStatusDate: Option[DateTime],
                                        tableNameOpt: Option[String] = Option(defaultTableName)
  ): Future[Map[String, Float]] = {
    val tableName = tableNameOpt.getOrElse(defaultTableName)
    val datePartitionColumn = if (tableName == defaultTableName) Some(defaultDatePartitionColumn) else None
    for {
      // Lookup-only costs for the requested workflow IDs
      workflowCosts <- executeWorkflowCostsQuery(
        workflowIds,
        googleProjectId,
        submissionDate,
        terminalStatusDate,
        tableName,
        datePartitionColumn
      )
    } yield extractCostResults(workflowCosts)
  }

  // modular method to save rows to WORKFLOW_ACTUAL_COST
  protected[dataaccess] def writeCostsToLocalDb(submissionId: UUID,
                                                costs: Map[String, Float],
                                                notFoundWorkflows: Set[String]
  ): Future[Int] = {
    val allExternalIds = costs.keySet ++ notFoundWorkflows
    dataSource.inTransaction { dataAccess =>
      import dataAccess.driver.api._
      for {
        // look up the internal workflow ids (Longs) for these external ids (Strings)
        workflowRecords <- dataAccess.workflowQuery
          .findWorkflowByExternalIdsAndSubmissionId(allExternalIds, submissionId)
          .result
        // map this submission's external ids to internal ids
        internalIdMap: Map[String, Long] = workflowRecords
          .filter(_.externalId.isDefined)
          .map(r => r.externalId.get -> r.id)
          .toMap
        // generate records for the found costs
        foundCosts: Seq[WorkflowActualCostRecord] = costs.map {
          case (externalId: String, cost: Float) if internalIdMap.contains(externalId) =>
            WorkflowActualCostRecord(internalIdMap(externalId), externalId, Option(cost))
        }.toSeq
        // generate records for the not-found costs
        notFoundCosts: Seq[WorkflowActualCostRecord] = notFoundWorkflows.toSeq.map {
          case externalId if internalIdMap.contains(externalId) =>
            WorkflowActualCostRecord(internalIdMap(externalId), externalId, None)
        }
        numRowsWritten <- dataAccess.workflowActualCostRawSqlQuery.safeInsert(foundCosts ++ notFoundCosts)
      } yield numRowsWritten
    }
  }

  /*
   * Manipulates and massages a BigQuery result.
   */
  def extractCostResults(rows: util.List[TableRow]): Map[String, Float] =
    Option(rows) match {
      case Some(rows) =>
        rows.asScala.map { row =>
          // workflow ID is contained in the 2nd cell, cost is contained in the 3rd cell
          row.getF.get(1).getV.toString -> row.getF.get(2).getV.toString.toFloat
        }.toMap
      case None => Map.empty[String, Float]
    }

  private def partitionDateClause(submissionDate: DateTime,
                                  terminalStatusDate: Option[DateTime],
                                  customDatePartitionColumn: Option[String]
  ): String = {
    // The Broad table uses a view with a different column name.
    val datePartitionColumn = customDatePartitionColumn.getOrElse("_PARTITIONDATE")

    // subtract a day so we never have to deal with timezones
    val windowStartDate = submissionDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    val windowEndDate = terminalStatusDate
      // if this submission has no date at which it reached terminal state, use the default window from config
      .getOrElse(submissionDate.plusDays(billingSearchWindowDays))
      // add a day so we never have to deal with timezones
      .plusDays(1)
      .toString(DateTimeFormat.forPattern("yyyy-MM-dd"))

    s"""AND $datePartitionColumn BETWEEN "$windowStartDate" AND "$windowEndDate""""
  }

  /*
   * Queries BigQuery for compute costs associated with the workflowIds.
   */
  private def executeWorkflowCostsQuery(workflowIds: Seq[String],
                                        googleProjectId: GoogleProjectId,
                                        submissionDate: DateTime,
                                        terminalStatusDate: Option[DateTime],
                                        tableName: String,
                                        datePartitionColumn: Option[String]
  ): Future[util.List[TableRow]] =
    workflowIds match {
      case Seq() => Future.successful(Seq.empty.asJava)
      case ids   =>
        val subquery = ids.map(_ => s"""workflowId LIKE ?""").mkString(" OR ")
        val querySql: String =
          generateWorkflowCostsQuery(submissionDate, terminalStatusDate, subquery, tableName, datePartitionColumn)

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
          logger.debug(
            s"Queried for costs of $idCount Workflow IDs: $rowsReturned Rows Returned and $bytesProcessed Bytes Processed."
          )
          Option(result.getRows).getOrElse(List.empty[TableRow].asJava)
        }
    }

  def generateSubmissionCostsQuery(submissionId: String,
                                   submissionDate: DateTime,
                                   terminalStatusDate: Option[DateTime],
                                   tableName: String,
                                   datePartitionColumn: Option[String]
  ): String =
    s"""SELECT wflabels.key, REPLACE(wflabels.value, "cromwell-", "") as `workflowId`, SUM(billing.cost)
       |FROM `$tableName` as billing, UNNEST(labels) as wflabels
       |CROSS JOIN UNNEST(billing.labels) as blabels
       |WHERE blabels.value = "terra-$submissionId"
       |AND wflabels.key = "cromwell-workflow-id"
       |AND project.id = ?
       |${partitionDateClause(submissionDate, terminalStatusDate, datePartitionColumn)}
       |GROUP BY wflabels.key, workflowId""".stripMargin

  def generateWorkflowCostsQuery(submissionDate: DateTime,
                                 terminalStatusDate: Option[DateTime],
                                 subquery: String,
                                 tableName: String,
                                 datePartitionColumn: Option[String]
  ): String =
    s"""|SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
        |FROM `$tableName`, UNNEST(labels) as labels
        |WHERE project.id = ?
        |AND labels.key LIKE "cromwell-workflow-id"
        |${partitionDateClause(submissionDate, terminalStatusDate, datePartitionColumn)}
        |GROUP BY labels.key, workflowId
        |HAVING $subquery""".stripMargin

  private def executeBigQuery(querySql: String, queryParams: List[QueryParameter]): Future[GetQueryResultsResponse] =
    for {
      jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject(serviceProject), querySql, queryParams, "POSITIONAL")
      job <- bigQueryDAO.getQueryStatus(jobRef)
      result <- bigQueryDAO.getQueryResult(job)
    } yield result
}
