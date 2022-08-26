package org.broadinstitute.dsde.rawls.mock

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.{ExecutionContext, Future}

class MockBondApiDAO(bondBaseUrl: String)(implicit executionContext: ExecutionContext) extends BondApiDAO {
  override def getBondProviders(): Future[List[String]] = Future.successful(List("fence", "dcf-fence"))

  override def getServiceAccountKey(provider: String, userInfo: UserInfo): Future[Option[BondResponseData]] =
    if (provider == "fence") {
      Future.successful(None)
    } else {
      Future.successful(Option(BondResponseData(BondServiceAccountEmail(s"$provider@bar.com"))))
    }

}
