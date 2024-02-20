package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsResolver
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.Future

class DisabledMarthaResolver extends DrsResolver {
  override def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] =
    throw new NotImplementedError("drsServiceAccountEmail is not implemented for Azure.")
}

