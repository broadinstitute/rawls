package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.Future


trait ResourceBufferDAO {

  def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): Future[GoogleProjectId]

  def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType): ProjectPoolId

}

