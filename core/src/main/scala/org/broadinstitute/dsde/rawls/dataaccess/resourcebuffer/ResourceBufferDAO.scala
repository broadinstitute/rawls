package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId}

import scala.concurrent.Future


trait ResourceBufferDAO {

  def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): Future[GoogleProjectId]

}

