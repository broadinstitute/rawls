package org.broadinstitute.dsde.rawls.resourcebuffer

import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.Future

trait ResourceBufferService {
  def getGoogleProjectFromBuffer(projectPoolType: ProjectPoolType = ProjectPoolType.Regular,
                                 handoutRequestId: String
  ): Future[GoogleProjectId]
  def toProjectPoolId(projectPoolType: ProjectPoolType): ProjectPoolId

  def serviceAccountEmail: String
}
