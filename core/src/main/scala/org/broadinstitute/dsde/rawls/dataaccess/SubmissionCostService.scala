package org.broadinstitute.dsde.rawls.dataaccess

import java.util

import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import com.google.api.services.bigquery.model.{QueryParameter, QueryParameterType, QueryParameterValue, TableRow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object SubmissionCostService {
  def constructor(tableName: String, serviceProject: String, bigQueryDAO: GoogleBigQueryDAO)(implicit executionContext: ExecutionContext) =
    new SubmissionCostService(tableName, serviceProject, bigQueryDAO)
}

class SubmissionCostService(tableName: String, serviceProject: String, bigQueryDAO: GoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {


  def getWorkflowCosts(workflowIds: Seq[String],
                       workspaceNamespace: String): Future[Map[String, Float]] = {

    extractWorkflowCostResults(executeWorkflowCostsQuery(workflowIds, workspaceNamespace))
  }

  /*
   * Manipulates and massages a BigQuery result.
   */
  def extractWorkflowCostResults(rowsFuture: Future[util.List[TableRow]]): Future[Map[String, Float]] = {

    rowsFuture map { rowsOrNull =>
      Option(rowsOrNull) match {
        case Some(rows) => rows.asScala.map { row =>
          // workflow ID is contained in the 2nd cell, cost is contained in the 3rd cell
          row.getF.get(1).getV.toString -> row.getF.get(2).getV.toString.toFloat
        }.toMap
        case None => Map.empty[String, Float]
      }
    }
  }

  /*
   * Queries BigQuery for compute costs associated with the workflowIds.
   */
  private def executeWorkflowCostsQuery(workflowIds: Seq[String],
                                        workspaceNamespace: String): Future[util.List[TableRow]] = {
    workflowIds match {
      case Seq() => Future.successful(Seq.empty.asJava)
      case ids =>
        val subquery = ids.map(_ => s"""workflowId LIKE ?""").mkString(" OR ")
        val querySql: String =
          s"""|SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
              |FROM `$tableName`, UNNEST(labels) as labels
              |WHERE project.id = ?
              |AND labels.key LIKE "cromwell-workflow-id"
              |GROUP BY labels.key, workflowId
              |HAVING $subquery""".stripMargin
        val stringParamType = new QueryParameterType().setType("STRING")
        val namespaceParam =
          new QueryParameter()
            .setParameterType(stringParamType)
            .setParameterValue(new QueryParameterValue().setValue(workspaceNamespace))
        val subqueryParams = workflowIds.toList map { workflowId =>
          new QueryParameter()
            .setParameterType(stringParamType)
            .setParameterValue(new QueryParameterValue().setValue(s"%$workflowId%"))
        }
        val queryParameters: List[QueryParameter] = namespaceParam :: subqueryParams

        for {
          jobRef <- bigQueryDAO.startParameterizedQuery(GoogleProject(serviceProject), querySql, queryParameters, "POSITIONAL")
          job <- bigQueryDAO.getQueryStatus(jobRef)
          result <- bigQueryDAO.getQueryResult(job)
        } yield {
          val idCount = ids.length
          val rowsReturned = result.getTotalRows
          val bytesProcessed = result.getTotalBytesProcessed
          logger.debug(s"Queried for costs of $idCount Workflow IDs: $rowsReturned Rows Returned and $bytesProcessed Bytes Processed.")

          result.getRows
        }
    }
  }
}
