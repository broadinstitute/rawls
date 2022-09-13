package org.broadinstitute.dsde.rawls.google

import com.google.api.services.accesscontextmanager.v1.model.Operation
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName

import scala.concurrent.Future

class MockGoogleAccessContextManagerDAO extends AccessContextManagerDAO {
  override def pollOperation(operationId: String): Future[Operation] =
    Future.successful(new Operation().setDone(true).setName("mock-poll-operation"))

  override def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName,
                                                   billingProjectNumbers: Set[String]
  ): Future[Operation] = Future.successful(new Operation().setDone(true).setName("mock-overwrite-projects-operation"))
}
