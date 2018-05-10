package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model.{GetQueryResultsResponse, Job, JobReference}
import org.broadinstitute.dsde.workbench.google.HttpGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class MockHttpGoogleBigQueryDAO(appName: String,
                                googleCredentialMode: GoogleCredentialMode,
                                workbenchMetricBaseName: String)
                               (implicit system: ActorSystem, executionContext: ExecutionContext)
  extends HttpGoogleBigQueryDAO(appName, googleCredentialMode, workbenchMetricBaseName) {

  override def startQuery(project: GoogleProject, querySql: String): Future[JobReference] = Future.successful(None[JobReference])

  override def getQueryStatus(jobRef: JobReference): Future[Job] = Future.successful(None[Job])

  override def getQueryResult(job: Job): Future[GetQueryResultsResponse] = Future.successful(None[GetQueryResultsResponse])

}