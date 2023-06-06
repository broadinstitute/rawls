package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}

import scala.concurrent.Future

trait ResourceBufferDAO {

  def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId]

}
