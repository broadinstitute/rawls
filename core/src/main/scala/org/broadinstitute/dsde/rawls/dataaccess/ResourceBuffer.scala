package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType

import scala.concurrent.Future

trait ResourceBuffer {
  def getGoogleProjectFromBuffer(projectPoolType: ProjectPoolType = ProjectPoolType.Regular,
                                 handoutRequestId: String
                                ): Future[GoogleProjectId]
  def toProjectPoolId(projectPoolType: ProjectPoolType): ProjectPoolId
}
