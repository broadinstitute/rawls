package org.broadinstitute.dsde.rawls.google

import com.google.api.services.accesscontextmanager.v1.model.Operation
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName

import scala.concurrent.Future

trait AccessContextManagerDAO {
  def pollOperation(operationId: String): Future[Operation]

  def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName,
                                          billingProjectNumbers: Set[String]
  ): Future[Operation]
}
