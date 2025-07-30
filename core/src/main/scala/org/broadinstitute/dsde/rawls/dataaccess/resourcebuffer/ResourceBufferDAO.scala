package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import bio.terra.buffer.model.JobModel
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}

import scala.concurrent.Future

trait ResourceBufferDAO {

  def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId]

  def repairResource(googleProjectId: String): Future[JobModel]
}
