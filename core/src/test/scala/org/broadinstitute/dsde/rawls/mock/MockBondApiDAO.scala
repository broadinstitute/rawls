package org.broadinstitute.dsde.rawls.mock

import org.broadinstitute.dsde.rawls.dataaccess.{BondResponseData, _}
import org.broadinstitute.dsde.rawls.model._
import scala.concurrent.{ExecutionContext, Future}

class MockBondApiDAO(bondBaseUrl: String)(implicit executionContext: ExecutionContext) extends BondApiDAO {
  override def getBondProviders(): Future[List[String]] = Future.successful(List("fence", "dcf-fence"))

  val bondResponseData =  BondResponseData(BondServiceAccountEmail("foo@bar.com"))
  override def getServiceAccountKey(provider: String, userInfo: UserInfo): Future[Option[BondResponseData]] = Future.successful(Option(bondResponseData))

}

