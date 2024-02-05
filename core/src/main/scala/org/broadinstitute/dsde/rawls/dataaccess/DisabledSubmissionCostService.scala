package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.bigquery.model._
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.joda.time.DateTime

import java.util
import scala.concurrent.Future

object DisabledSubmissionCostService {
  def constructor = new DisabledSubmissionCostService
}

class DisabledSubmissionCostService
  extends SubmissionCost {

  val stringParamType: QueryParameterType = new QueryParameterType().setType("STRING")

  def getSubmissionCosts(submissionId: String,
                         workflowIds: Seq[String],
                         googleProjectId: GoogleProjectId,
                         submissionDate: DateTime,
                         terminalStatusDate: Option[DateTime],
                         tableNameOpt: Option[String]
                        ): Future[Map[String, Float]] =
    throw new NotImplementedError("getSubmissionCosts is not implemented for Azure.")

  def getWorkflowCost(workflowId: String,
                      googleProjectId: GoogleProjectId,
                      submissionDate: DateTime,
                      terminalStatusDate: Option[DateTime],
                      tableNameOpt: Option[String]
                     ): Future[Map[String, Float]] =
    throw new NotImplementedError("getWorkflowCost is not implemented for Azure.")

  def extractCostResults(rows: util.List[TableRow]): Map[String, Float] =
    throw new NotImplementedError("extractCostResults is not implemented for Azure.")


  def generateSubmissionCostsQuery(submissionId: String,
                                   submissionDate: DateTime,
                                   terminalStatusDate: Option[DateTime],
                                   tableName: String,
                                   datePartitionColumn: Option[String]
                                  ): String =
    throw new NotImplementedError("generateSubmissionCostsQuery is not implemented for Azure.")

  def generateWorkflowCostsQuery(submissionDate: DateTime,
                                 terminalStatusDate: Option[DateTime],
                                 subquery: String,
                                 tableName: String,
                                 datePartitionColumn: Option[String]
                                ): String =
    throw new NotImplementedError("generateWorkflowCostsQuery is not implemented for Azure.")
}

