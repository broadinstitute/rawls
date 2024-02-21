package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, ProjectPoolId}

import scala.concurrent.Future

class DisabledHttpResourceBufferDAO extends ResourceBufferDAO {
  override def handoutGoogleProject(projectPoolId: ProjectPoolId, handoutRequestId: String): Future[GoogleProjectId] =
    throw new NotImplementedError("handoutGoogleProject is not implemented for Azure.")
}
