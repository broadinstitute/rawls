package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import bio.terra.buffer.model.{JobModel, SqlSortDirectionDescDefault}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}

import scala.concurrent.Future

trait ResourceBufferDAO {

  def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId]

  def repairResource(googleProjectId: String): Future[JobModel]

  def enumerateJobs(offset: Integer,
                    limit: Integer,
                    direction: SqlSortDirectionDescDefault,
                    className: String,
                    inputs: java.util.List[String]): Future[java.util.List[JobModel]]

  def getJob(jobId: String): Future[JobModel]

  def getJobResult(jobId: String): Future[Object]
}
