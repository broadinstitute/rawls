package org.broadinstitute.dsde.rawls.serviceperimeter

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName
import org.broadinstitute.dsde.rawls.util.Retry

trait ServicePerimeterService extends LazyLogging with Retry {
  def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName,
                                         dataAccess: DataAccess
  ): ReadWriteAction[Unit]
}
