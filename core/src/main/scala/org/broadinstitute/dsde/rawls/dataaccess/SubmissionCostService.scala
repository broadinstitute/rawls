package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.bigquery.model._
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.joda.time.DateTime

import java.util
import scala.concurrent.Future

trait SubmissionCostService {

  val stringParamType: QueryParameterType

  def getSubmissionCosts(submissionId: String,
                         workflowIds: Seq[String],
                         googleProjectId: GoogleProjectId,
                         submissionDate: DateTime,
                         terminalStatusDate: Option[DateTime],
                         tableNameOpt: Option[String]
  ): Future[Map[String, Float]]

  def getWorkflowCost(workflowId: String,
                      googleProjectId: GoogleProjectId,
                      submissionDate: DateTime,
                      terminalStatusDate: Option[DateTime],
                      tableNameOpt: Option[String]
  ): Future[Map[String, Float]]

  def extractCostResults(rows: util.List[TableRow]): Map[String, Float]

  def generateSubmissionCostsQuery(submissionId: String,
                                   submissionDate: DateTime,
                                   terminalStatusDate: Option[DateTime],
                                   tableName: String,
                                   datePartitionColumn: Option[String]
  ): String

  def generateWorkflowCostsQuery(submissionDate: DateTime,
                                 terminalStatusDate: Option[DateTime],
                                 subquery: String,
                                 tableName: String,
                                 datePartitionColumn: Option[String]
  ): String
}
