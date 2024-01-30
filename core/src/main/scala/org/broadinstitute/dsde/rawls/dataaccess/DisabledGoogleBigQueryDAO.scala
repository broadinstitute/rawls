package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes._
import org.broadinstitute.dsde.workbench.google.{AbstractHttpGoogleDAO, GoogleBigQueryDAO}
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class DisabledGoogleBigQueryDAO(
                             appName: String,
                             googleCredentialMode: GoogleCredentialMode,
                             workbenchMetricBaseName: String
                           )(implicit system: ActorSystem, executionContext: ExecutionContext)
  extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName)
    with GoogleBigQueryDAO {

  override val scopes = Seq(BigqueryScopes.BIGQUERY)

  implicit override val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.BigQuery

  private lazy val bigquery: Bigquery =
    new Bigquery.Builder(httpTransport, jsonFactory, googleCredential).setApplicationName(appName).build()

  private def submitQuery(projectId: String, job: Job): Future[JobReference] =
    throw new NotImplementedError("submitQuery is not implemented for Azure.")

  override def startQuery(project: GoogleProject, querySql: String): Future[JobReference] =
    throw new NotImplementedError("startQuery is not implemented for Azure.")

  override def startParameterizedQuery(project: GoogleProject,
                                       querySql: String,
                                       queryParameters: List[QueryParameter],
                                       parameterMode: String
                                      ): Future[JobReference] =
    throw new NotImplementedError("startParameterizedQuery is not implemented for Azure.")

  override def getQueryStatus(jobRef: JobReference): Future[Job] =
    throw new NotImplementedError("getQueryStatus is not implemented for Azure.")

  override def getQueryResult(job: Job): Future[GetQueryResultsResponse] =
    throw new NotImplementedError("getQueryResult is not implemented for Azure.")
}
