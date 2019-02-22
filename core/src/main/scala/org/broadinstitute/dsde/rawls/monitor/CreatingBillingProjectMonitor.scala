package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor._
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * Created by dvoet on 8/22/16.
 */
object CreatingBillingProjectMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, pubSubDAO: GooglePubSubDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate, requesterPaysRole: String, dmPubSubTopic: String, dmPubSubSubscription: String)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, pubSubDAO, samDAO, projectTemplate, requesterPaysRole, dmPubSubTopic, dmPubSubSubscription))
  }

  //shiny new Deployment Manager flow
  sealed trait CreatingBillingProjectMonitorMessage
  case object Startup extends CreatingBillingProjectMonitorMessage
  case object CheckPubSub extends CreatingBillingProjectMonitorMessage

  //old, operation-based way of monitoring BPs
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val pubSubDAO: GooglePubSubDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate, val requesterPaysRole: String, val dmPubSubTopic: String, val dmPubSubSubscription: String)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
  self ! Startup

  override def receive = {
    case Startup => startup pipeTo self
    case CheckPubSub => checkPubSub pipeTo self

    case CheckDone(creatingCount) if creatingCount > 0 => context.system.scheduler.scheduleOnce(5 seconds, self, CheckPubSub)
    case CheckDone(creatingCount) => context.system.scheduler.scheduleOnce(1 minute, self, CheckPubSub)

    case Failure(t) =>
      logger.error(s"failure monitoring creating billing projects", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckPubSub)
  }
}

trait CreatingBillingProjectMonitor extends LazyLogging {
  val datasource: SlickDataSource
  val gcsDAO: GoogleServicesDAO
  val pubSubDAO: GooglePubSubDAO
  val projectTemplate: ProjectTemplate
  val samDAO: SamDAO
  val requesterPaysRole: String
  val dmPubSubTopic: String
  val dmPubSubSubscription: String

  def startup()(implicit executionContext: ExecutionContext): Future[CreatingBillingProjectMonitorMessage] = {
    for {
      _ <- pubSubDAO.createTopic(dmPubSubTopic)
      _ <- pubSubDAO.createSubscription(dmPubSubTopic, dmPubSubSubscription)
    } yield {
      CheckPubSub
    }
  }

  def updateBillingProjects(projectMap: Map[RawlsBillingProjectName, GooglePubSubDAO.PubSubMessage])(implicit executionContext: ExecutionContext): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      //these messages will include info from all projects from all rawlses, not just this one.
      // get the BPs we know about in the db and iterate over them.
      dataAccess.rawlsBillingProjectQuery.getBillingProjects(projectMap.keySet) flatMap { projectsOwnedByMe =>
        val bpUpdates = projectsOwnedByMe map { project =>
          if (projectMap(project.projectName).attributes("status") == "ERROR") {
            //assuming that the error is showing up in the body of the message here.
            project.copy(status = CreationStatuses.Error, message = Some(projectMap(project.projectName).contents))
          } else {
            //TODO: delete the deployment after the project has been created fine, as there's a limit (~1000)
            //this will require setting up the deployment to be deletable (else deleting the deployment will delete the project too)
            project.copy(status = CreationStatuses.Ready)
          }
        }
        dataAccess.rawlsBillingProjectQuery.updateBillingProjects(bpUpdates)
      }
    }.mapTo[Unit]
  }

  def checkPubSub()(implicit executionContext: ExecutionContext): Future[CreatingBillingProjectMonitorMessage] = {
    for {
      psMessages <- pubSubDAO.pullMessages(dmPubSubSubscription, 100)
      projectMap = psMessages.map(m => RawlsBillingProjectName(m.attributes("projectId")) -> m ).toMap
      _ <- updateBillingProjects(projectMap)
      _ <- pubSubDAO.acknowledgeMessages(dmPubSubSubscription, psMessages)
    } yield {
      CheckDone(psMessages.size)
    }
  }
}
