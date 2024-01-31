package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.accesscontextmanager.v1.model.Operation
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName

import scala.concurrent.Future

class DisabledHttpGoogleAccessContextManagerDAO extends AccessContextManagerDAO {
  def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName, billingProjectNumbers: Set[String]): Future[Operation] =
    throw new NotImplementedError("overwriteProjectsInServicePerimeter method is not implemented for Azure.")
  override def pollOperation(operationId: String): Future[Operation] =
    throw new NotImplementedError("pollOperation method is not implemented for Azure.")
}

