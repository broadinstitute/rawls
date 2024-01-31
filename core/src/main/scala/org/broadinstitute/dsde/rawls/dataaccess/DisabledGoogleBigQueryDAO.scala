package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.bigquery.model._
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

class DisabledGoogleBigQueryDAO extends GoogleBigQueryDAO {
  override def startQuery(project: GoogleProject, querySql: String): Future[JobReference] =
    throw new NotImplementedError("startQuery is not implemented for Azure.")
  override def startParameterizedQuery(project: GoogleProject, querySql: String, queryParameters: List[QueryParameter], parameterMode: String): Future[JobReference] =
    throw new NotImplementedError("startParameterizedQuery is not implemented for Azure.")
  override def getQueryStatus(jobRef: JobReference): Future[Job] =
    throw new NotImplementedError("getQueryStatus is not implemented for Azure.")
  override def getQueryResult(job: Job): Future[GetQueryResultsResponse] =
    throw new NotImplementedError("getQueryResult is not implemented for Azure.")
}
