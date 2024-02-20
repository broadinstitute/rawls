package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.ResourceBuffer
import org.broadinstitute.dsde.rawls.model.ProjectPoolType.ProjectPoolType
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId, ProjectPoolType}

import scala.concurrent.Future

class DisabledResourceBufferService extends ResourceBuffer{
  def getGoogleProjectFromBuffer(projectPoolType: ProjectPoolType = ProjectPoolType.Regular,
                                 handoutRequestId: String
                                ): Future[GoogleProjectId] =
    throw new NotImplementedError("getGoogleProjectFromBuffer is not implemented for Azure.")
  def toProjectPoolId(projectPoolType: ProjectPoolType): ProjectPoolId =
    throw new NotImplementedError("toProjectPoolId is not implemented for Azure.")
}
