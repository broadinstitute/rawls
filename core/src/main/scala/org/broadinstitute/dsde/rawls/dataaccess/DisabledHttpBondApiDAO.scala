package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.UserInfo
import scala.concurrent.Future

class DisabledHttpBondApiDAO extends BondApiDAO {
  def getBondProviders(): Future[List[String]] =
    throw new NotImplementedError("getBondProviders is not implemented for Azure.")
  def getServiceAccountKey(provider: String, userInfo: UserInfo): Future[Option[BondResponseData]] =
    throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
}