package org.broadinstitute.dsde.rawls.mock

import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId}

import scala.concurrent.Future

class MockResourceBufferDAO extends ResourceBufferDAO {

  def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): Future[GoogleProjectId] = {
    val googleProjectId = GoogleProjectId("project-from-rbs")
    Future.successful(googleProjectId)
  }

}
