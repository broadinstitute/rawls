package org.broadinstitute.dsde.rawls.dataaccess

import java.util
import com.google.api.services.bigquery.model.TableRow
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, Future}

object SubmissionCostService {
  def constructor(bigQueryDAO: HttpGoogleBigQueryDAO)(implicit executionContext: ExecutionContext) =
    new SubmissionCostService(bigQueryDAO)
}

class SubmissionCostService(bigQueryDAO: HttpGoogleBigQueryDAO)(implicit val executionContext: ExecutionContext) {

  def getWorkflowCosts(namespace: String,
                       workflowIds: Seq[String],
                       userInfo: UserInfo,
                       googleProject: GoogleProject): Future[Map[String, Float]] = {

    val tableName = "broad-gcp-billing:gcp_billing_export.gcp_billing_export_v1_001AC2_2B914D_822931"
    val subqueryTemplate = workflowIds.map(id => s"labels_value LIKE %$id%").mkString(" OR ")
    val queryString = "SELECT GROUP_CONCAT(labels.key) WITHIN RECORD AS labels_key," +
                        " GROUP_CONCAT(labels.value) WITHIN RECORD AS labels_value," +
                        " cost" +
                        s" FROM [$tableName]" +
                        s" WHERE project.id = '$namespace'" +
                        " AND labels.key IN (\"cromwell-workflow-id\"," +
                                          " \"cromwell-workflow-name\"," +
                                          " \"cromwell-subworkflow-name\"," +
                                          " \"wdl-task-name\"," +
                                          " \"wdl-call-alias\")" +
                        s" HAVING $subqueryTemplate"
                        // uncomment for quick testing:
                        //+ " LIMIT 1"

    val rowsFuture: Future[util.List[TableRow]] = for {
      jobRef <- bigQueryDAO.startQuery(googleProject, queryString)
      job <- bigQueryDAO.getQueryStatus(jobRef)
      result <- bigQueryDAO.getQueryResult(job)
    } yield result.getRows

    val rowsListFuture = rowsFuture map { rows =>
      JavaConverters.iterableAsScalaIterable(rows).toList
    }

    rowsListFuture map { rowsList =>
      // workflow ID is contained in the 2nd cell, cost is contained in the 3rd cell
      rowsList.map(row => row.getF.get(1).getV.toString -> row.getF.get(2).getV.toString.toFloat).toMap
    }

    // TODO match each entry in workflowIds to those in the costMap and aggregate costs for each corresponding costMap entry
  }
}
