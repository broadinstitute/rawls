package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.bigquery.model._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.joda.time.DateTime

import java.util
import scala.concurrent.{ExecutionContext, Future}

object DisabledSubmissionCostService {
  def constructor(defaultTableName: String,
                  defaultDatePartitionColumn: String,
                  serviceProject: String,
                  billingSearchWindowDays: Int,
                  bigQueryDAO: GoogleBigQueryDAO
                 )(implicit executionContext: ExecutionContext) =
    new SubmissionCostService(defaultTableName,
      defaultDatePartitionColumn,
      serviceProject,
      billingSearchWindowDays,
      bigQueryDAO
    )
}

class DisabledSubmissionCostService(defaultTableName: String,
                            defaultDatePartitionColumn: String,
                            serviceProject: String,
                            billingSearchWindowDays: Int,
                            bigQueryDAO: GoogleBigQueryDAO
                           )(implicit val executionContext: ExecutionContext)
  extends LazyLogging {

  val stringParamType: QueryParameterType = new QueryParameterType().setType("STRING")

  def getSubmissionCosts(submissionId: String,
                         workflowIds: Seq[String],
                         googleProjectId: GoogleProjectId,
                         submissionDate: DateTime,
                         terminalStatusDate: Option[DateTime],
                         tableNameOpt: Option[String] = Option(defaultTableName)
                        ): Future[Map[String, Float]] =
    throw new NotImplementedError("getSubmissionCosts is not implemented for Azure.")

  def getWorkflowCost(workflowId: String,
                      googleProjectId: GoogleProjectId,
                      submissionDate: DateTime,
                      terminalStatusDate: Option[DateTime],
                      tableNameOpt: Option[String] = Option(defaultTableName)
                     ): Future[Map[String, Float]] =
    throw new NotImplementedError("getWorkflowCost is not implemented for Azure.")

  def extractCostResults(rows: util.List[TableRow]): Map[String, Float] =
    throw new NotImplementedError("extractCostResults is not implemented for Azure.")

  private def partitionDateClause(submissionDate: DateTime,
                                  terminalStatusDate: Option[DateTime],
                                  customDatePartitionColumn: Option[String]
                                 ): String =
    throw new NotImplementedError("partitionDateClause is not implemented for Azure.")

  private def executeSubmissionCostsQuery(submissionId: String,
                                          googleProjectId: GoogleProjectId,
                                          submissionDate: DateTime,
                                          terminalStatusDate: Option[DateTime],
                                          tableName: String,
                                          datePartitionColumn: Option[String]
                                         ): Future[util.List[TableRow]] =
    throw new NotImplementedError("executeSubmissionCostsQuery is not implemented for Azure.")
  private def executeWorkflowCostsQuery(workflowIds: Seq[String],
                                        googleProjectId: GoogleProjectId,
                                        submissionDate: DateTime,
                                        terminalStatusDate: Option[DateTime],
                                        tableName: String,
                                        datePartitionColumn: Option[String]
                                       ): Future[util.List[TableRow]] =
    throw new NotImplementedError("executeWorkflowCostsQuery is not implemented for Azure.")

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

  private def executeBigQuery(querySql: String, queryParams: List[QueryParameter]): Future[GetQueryResultsResponse] =
    throw new NotImplementedError("executeBigQuery is not implemented for Azure.")
}

