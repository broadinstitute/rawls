package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import bio.terra.rbs.generated.model.PoolInfo
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PoolId, ProjectPoolId, ProjectPoolType}


trait ResourceBufferDAO {

  def getPoolInfo(poolId: PoolId): PoolInfo

  def handoutGoogleProject(poolId: PoolId, handoutRequestId: String): GoogleProjectId

  def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType): ProjectPoolId

}

