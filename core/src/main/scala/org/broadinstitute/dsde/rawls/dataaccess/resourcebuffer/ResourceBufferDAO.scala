package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import bio.terra.rbs.generated.model.PoolInfo
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}


trait ResourceBufferDAO {

  def getPoolInfo(poolId: String): PoolInfo

  def handoutGoogleProject(poolId: String, handoutRequestId: String): GoogleProjectId

  def getProjectPoolId(projectPoolType: ProjectPoolType.ProjectPoolType): ProjectPoolId

}

