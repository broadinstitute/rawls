package org.broadinstitute.dsde.rawls.google

import com.google.api.services.accesscontextmanager.v1beta.model.Operation
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName

import scala.concurrent.Future

trait AccessContextManagerDAO {
  def pollOperation(operationId: String): Future[Operation]

  def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName, billingProjectNumbers: Seq[String]): Future[Operation]
}
