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
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate, requesterPaysRole: String)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, samDAO, projectTemplate, requesterPaysRole))
  }

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate, val requesterPaysRole: String)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
  self ! CheckNow

  override def receive = {
    case CheckNow => checkCreatingProjects pipeTo self

    case CheckDone(creatingCount) if creatingCount > 0 => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)
    case CheckDone(creatingCount) => context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring creating billing projects", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
  }
}

trait CreatingBillingProjectMonitor extends LazyLogging {
  val datasource: SlickDataSource
  val gcsDAO: GoogleServicesDAO
  val projectTemplate: ProjectTemplate
  val samDAO: SamDAO
  val requesterPaysRole: String

  def checkCreatingProjects()(implicit executionContext: ExecutionContext): Future[CheckDone] = {
    for {
      (creatingProjects, operations) <- datasource.inTransaction { dataAccess =>
        for {
          projects <- dataAccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(CreationStatuses.Creating)
          operations <- dataAccess.rawlsBillingProjectQuery.loadOperationsForProjects(projects.map(_.projectName))
        } yield {
          (projects, operations)
        }
      }
      updatedProjectCount <- setupProjects(creatingProjects, operations)
    } yield {
      CheckDone(creatingProjects.size - updatedProjectCount)
    }
  }

  def setupProjects(projects: Seq[RawlsBillingProject],
                    operations: Seq[RawlsBillingProjectOperationRecord])(implicit executionContext: ExecutionContext): Future[Int] = {

    for {
      updatedOperations <- Future.traverse(operations) {
        case operation@RawlsBillingProjectOperationRecord(_, _, _, true, _, _) => Future.successful(operation)
        case operation@RawlsBillingProjectOperationRecord(_, _, _, false, _, _) => gcsDAO.pollOperation(operation).recover {
          // if we don't mark this as done we might continue retrying forever and pollOperation already does some retrying
          case t: Throwable => operation.copy(done = true, errorMessage = Option(s"error getting ${operation.operationName} operation status: ${t.getMessage}"))
        }
      }

      // save the updates
      _ <- datasource.inTransaction { dataAccess =>
        val changedOperations = updatedOperations.toSet -- operations.toSet
        dataAccess.rawlsBillingProjectQuery.updateOperations(changedOperations.toSeq)
      }

      operationsByProject = updatedOperations.groupBy(rec => RawlsBillingProjectName(rec.projectName))

      maybeUpdatedProjects <- Future.traverse(projects) { project =>
        // figure out if the project creation is done yet and set the project status accordingly
        val nextStepFuture = operationsByProject(project.projectName) match {
          case Seq() =>
            // there are no operations, there is a small window when this can happen but it should resolve itself so let it pass
            Future.successful(project)

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.DEPLOYMENT_MANAGER_CREATE_PROJECT, _, true, None, _)) =>
            // create project operation finished successfully
            gcsDAO.cleanupDMProject(project.projectName) map { _ =>
              project.copy(status = CreationStatuses.Ready)
            }

          case fail@Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.DEPLOYMENT_MANAGER_CREATE_PROJECT, _, true, Some(error), _)) =>
            // create project operation finished with an error
            logger.debug(s"project ${project.projectName.value} creation finished with errors: $error")
            gcsDAO.cleanupDMProject(project.projectName) map { _ =>
              project.copy(status = CreationStatuses.Error, message = Option(s"project ${project.projectName.value} creation finished with errors: $error \n ${fail.head}"))
            }

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.CREATE_PROJECT_OPERATION, _, _, _, _)) =>
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(s"failure processing new project ${project.projectName.value}: old-style project creation no longer supported")))

          case bp :: bp2 :: bps =>
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(s"failure processing new project ${project.projectName.value}: multiple BillingProjectOperationRecords (probably an old project)")))

          case _ =>
            // still running
            Future.successful(project)
        }

        nextStepFuture.recover {
          case t: Throwable =>
            logger.error(s"failure processing new project ${project.projectName.value}", t)
            project.copy(status = CreationStatuses.Error, message = Option(t.getMessage))
        }
      }

      _ <- datasource.inTransaction { dataAccess =>
        // save project updates
        val updatedProjects = maybeUpdatedProjects.toSet -- projects.toSet
        dataAccess.rawlsBillingProjectQuery.updateBillingProjects(updatedProjects)
      }
    } yield {
      maybeUpdatedProjects.count(project => CreationStatuses.terminal.contains(project.status))
    }
  }
}
