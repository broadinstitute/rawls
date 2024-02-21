package org.broadinstitute.dsde.rawls.disabled

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction}
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeter

import scala.concurrent.ExecutionContext

class DisabledServicePerimeterService(implicit
  val system: ActorSystem,
  protected val executionContext: ExecutionContext
) extends ServicePerimeter {
  override def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName,
                                                  dataAccess: DataAccess
  ): ReadAction[Unit] =
    throw new NotImplementedError("overwriteGoogleProjectsInPerimeter is not implemented for Azure.")
}
