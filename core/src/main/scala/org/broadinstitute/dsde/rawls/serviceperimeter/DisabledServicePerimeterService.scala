package org.broadinstitute.dsde.rawls.serviceperimeter

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.ServicePerimeterServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction}
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectNumber,
  RawlsBillingProject,
  RawlsRequestContext,
  ServicePerimeterName,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.Retry
import scala.concurrent.{ExecutionContext, Future}

class DisabledServicePerimeterService(dataSource: SlickDataSource,
                              gcsDAO: GoogleServicesDAO,
                              config: ServicePerimeterServiceConfig
                             )(implicit override val system: ActorSystem, override protected val executionContext: ExecutionContext)
  extends ServicePerimeterService(dataSource: SlickDataSource,
    gcsDAO: GoogleServicesDAO,
    config: ServicePerimeterServiceConfig){
  private def collectWorkspacesInPerimeter(servicePerimeterName: ServicePerimeterName,
                                           dataAccess: DataAccess
                                          ): ReadAction[Seq[Workspace]] =
    throw new NotImplementedError("collectWorkspacesInPerimeter is not implemented for Azure.")
  private def collectBillingProjectsInPerimeter(servicePerimeterName: ServicePerimeterName,
                                                dataAccess: DataAccess
                                               ): ReadAction[Seq[RawlsBillingProject]] =
    throw new NotImplementedError("collectBillingProjectsInPerimeter is not implemented for Azure.")
  private def loadStaticProjectsForPerimeter(servicePerimeterName: ServicePerimeterName): Seq[GoogleProjectNumber] =
    throw new NotImplementedError("loadStaticProjectsForPerimeter is not implemented for Azure.")

  override def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName,
                                                  dataAccess: DataAccess
                                        ): ReadAction[Unit] =
    throw new NotImplementedError("overwriteGoogleProjectsInPerimeter is not implemented for Azure.")
}

object DisabledServicePerimeterService {
  def checkServicePerimeterAccess(samDAO: SamDAO,
                                  servicePerimeterOption: Option[ServicePerimeterName],
                                  ctx: RawlsRequestContext
                                 )(implicit ec: ExecutionContext): Future[Unit] =
    throw new NotImplementedError("checkServicePerimeterAccess is not implemented for Azure.")
}
