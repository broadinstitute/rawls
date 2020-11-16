package org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.rbs.generated.model.PoolInfo
import org.broadinstitute.dsde.rawls.model.GoogleProjectId


trait ResourceBufferDAO {

  def getPoolInfo(poolId: String, accessToken: OAuth2BearerToken): PoolInfo

  def handoutGoogleProject(poolId: String, handoutRequestId: String, accessToken: OAuth2BearerToken): GoogleProjectId

}
