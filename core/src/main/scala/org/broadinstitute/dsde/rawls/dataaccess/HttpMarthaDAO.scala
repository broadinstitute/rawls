package org.broadinstitute.dsde.rawls.dataaccess

import scala.concurrent.{ExecutionContext, Future}

class HttpMarthaDAO(implicit executionContext: ExecutionContext) extends MarthaDAO {
  override def dosToGs(dos: String): Future[String] = ???
}
