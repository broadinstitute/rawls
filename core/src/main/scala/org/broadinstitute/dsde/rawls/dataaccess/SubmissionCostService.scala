package org.broadinstitute.dsde.rawls.dataaccess

import java.util
import com.google.api.services.bigquery.model.TableRow
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object SubmissionCostService {
  def constructor(tableName: String, bigQueryDAO: HttpGoogleBigQueryDAO)(implicit executionContext: ExecutionContext) =
    new SubmissionCostService(tableName, bigQueryDAO)
}

class SubmissionCostService(tableName: String, bigQueryDAO: HttpGoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) {


  def getWorkflowCosts(namespace: String,
                       workflowIds: Seq[String],
                       googleProject: GoogleProject): Future[Map[String, Float]] = {

    extractWorkflowCostResults(executeWorkflowCostsQuery(namespace, workflowIds, googleProject))
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
  private def executeWorkflowCostsQuery(namespace: String,
                       workflowIds: Seq[String],
                       googleProject: GoogleProject): Future[util.List[TableRow]] = {

    // TODO: need to protect against SQL injection. Can we use Slick? Hate to bring in Slick just for that.
    val subqueryTemplate = workflowIds.map(id => s"""workflowId LIKE "%$id%"""").mkString(" OR ")
    val queryString: String =
      s"""|SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as workflowId, SUM(cost)
        |FROM [$tableName]
        |WHERE project.id = '$namespace'
        |AND labels.key LIKE "cromwell-workflow-id"
        |GROUP BY labels.key, workflowId
        |HAVING $subqueryTemplate""".stripMargin
    //    + " LIMIT 1"  // uncomment for quick testing

    for {
      jobRef <- bigQueryDAO.startQuery(googleProject, queryString)
      job <- bigQueryDAO.getQueryStatus(jobRef)
      result <- bigQueryDAO.getQueryResult(job)
    } yield result.getRows
  }
}
