package org.broadinstitute.dsde.rawls.serviceperimeter

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.ServicePerimeterAccessException
import org.broadinstitute.dsde.rawls.config.ServicePerimeterServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, ErrorReport, GoogleProjectNumber, RawlsBillingProject, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo, Workspace}
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}


class ServicePerimeterService(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, config: ServicePerimeterServiceConfig)
                             (implicit val system: ActorSystem, protected val executionContext: ExecutionContext)
  extends LazyLogging with Retry {

  import dataSource.dataAccess.driver.api._

  /**
    * Look up all of the Workspaces contained in Billing Projects that use the specified
    * ServicePerimeterName
    *
    * @param servicePerimeterName
    * @return
    */
  private def collectWorkspacesInPerimeter(servicePerimeterName: ServicePerimeterName, dataAccess: DataAccess): ReadAction[Seq[Workspace]] = {
    dataAccess.workspaceQuery.getWorkspacesInPerimeter(servicePerimeterName)
  }

  /**
    * Look up all of the Billing Projects that use the specified
    * ServicePerimeterName
    *
    * @param servicePerimeterName
    * @return
    */
  private def collectBillingProjectsInPerimeter(servicePerimeterName: ServicePerimeterName, dataAccess: DataAccess): ReadAction[Seq[RawlsBillingProject]] = {
    dataAccess.rawlsBillingProjectQuery.listProjectsWithServicePerimeterAndStatus(servicePerimeterName, CreationStatuses.Ready)
  }

  /**
    * Some Service Perimeters may required that they have some additional non-Terra Google Projects that need to be in
    * the perimeter for some other reason.  These are provided to us by the Service Perimeter stakeholders and we add
    * them to the Rawls Config so that whenever we update the list of projects for a perimeter, these projects are
    * always included.
    *
    * @param servicePerimeterName
    * @return
    */
  private def loadStaticProjectsForPerimeter(servicePerimeterName: ServicePerimeterName): Seq[GoogleProjectNumber] = {
    config.staticProjectsInPerimeters.getOrElse(servicePerimeterName, Seq.empty)
  }

  /**
    * Takes the the name of a Service Perimeter as the only parameter.  Since multiple Billing Projects can specify the
    * same Service Perimeter, we will:
    * 1. Load all the Billing Projects that also use this servicePerimeterName
    * 2. Load all the Workspaces in all of those Billing Projects
    * 3. Collect all of the GoogleProjectNumbers from those Workspaces and Billing Projects
    * 4. Post that list to Google to overwrite the Service Perimeter's list of included Google Projects
    * 5. Poll until Google Operation to update the Service Perimeter gets to some terminal state
    * Throw exceptions if any of this goes awry
    *
    * @param servicePerimeterName
    * @return Future[Unit] indicating whether we succeeded to update the Service Perimeter
    */

  def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName, dataAccess: DataAccess): ReadAction[Unit] = {
    for {
      workspacesInPerimeter <- collectWorkspacesInPerimeter(servicePerimeterName, dataAccess)
      billingProjectsInPerimeter <- collectBillingProjectsInPerimeter(servicePerimeterName, dataAccess)
      googleProjectNumbers = workspacesInPerimeter.flatMap(_.googleProjectNumber) ++ billingProjectsInPerimeter.flatMap(_.googleProjectNumber) ++ loadStaticProjectsForPerimeter(servicePerimeterName)
      googleProjectNumberStrings = googleProjectNumbers.map(_.value).toSet
      operation <- DBIO.from(gcsDAO.accessContextManagerDAO.overwriteProjectsInServicePerimeter(servicePerimeterName, googleProjectNumberStrings))
      result <- DBIO.from(retryUntilSuccessOrTimeout(failureLogMessage = s"Google Operation to update Service Perimeter: ${servicePerimeterName} was not successful")(config.pollInterval, config.pollTimeout) { () =>
        gcsDAO.pollOperation(OperationId(GoogleApiTypes.AccessContextManagerApi, operation.getName)).map {
          case OperationStatus(false, _) => Future.failed(new RawlsException(s"Google Operation to update Service Perimeter ${servicePerimeterName} is still in progress..."))
          // TODO: If the operation to update the Service Perimeter failed, we need to consider the possibility that
          // the list of Projects in the Perimeter may have been wiped or somehow modified in an undesirable way.  If
          // this happened, it would be possible for Projects intended to be in the Perimeter are NOT in that
          // Perimeter anymore, which is a problem.
          case OperationStatus(true, errorMessage) if !errorMessage.isEmpty => Future.successful(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Google Operation to update Service Perimeter ${servicePerimeterName} failed with message: ${errorMessage}")))
          case _ => Future.successful()
        }
      })
    } yield {
      result match {
        case Left(regrets) =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "Failed to update perimeter", regrets.map(ErrorReport(_)).toList))
        case Right(_) => ()
      }
    }
  }
}

object ServicePerimeterService {
  def checkServicePerimeterAccess(samDAO: SamDAO, servicePerimeterOption: Option[ServicePerimeterName], userInfo: UserInfo)(implicit ec: ExecutionContext): Future[Unit] = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new ServicePerimeterAccessException(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }
}
