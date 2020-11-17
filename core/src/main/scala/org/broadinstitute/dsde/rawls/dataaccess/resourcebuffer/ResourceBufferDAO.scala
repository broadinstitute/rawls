package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import bio.terra.rbs.generated.model.PoolInfo
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.Future


trait ResourceBufferDAO {

  def getPoolInfo(poolId: PoolId): PoolInfo

  def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): Future[GoogleProjectId]

  def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType): ProjectPoolId

}

