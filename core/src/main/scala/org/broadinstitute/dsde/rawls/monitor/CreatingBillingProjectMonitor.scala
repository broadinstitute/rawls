package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, RawlsBillingProjectRecord}
import org.broadinstitute.dsde.rawls.dataaccess.{GooglePubSubDAO, ProjectTemplate, GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor.{CheckDone, CheckNow}
import scala.concurrent.duration._

import scala.concurrent.{Future, ExecutionContext}
import scala.util
import scala.util.Success

/**
 * Created by dvoet on 8/22/16.
 */
object CreatingBillingProjectMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, projectTemplate: ProjectTemplate)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, projectTemplate))
  }

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val projectTemplate: ProjectTemplate)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
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

  def checkCreatingProjects()(implicit executionContext: ExecutionContext): Future[CheckDone] = {
    for {
      (creatingProjects, groupEmailsByRef, operations) <- datasource.inTransaction { dataAccess =>
        for {
          projects <- dataAccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(CreationStatuses.Creating)
          groupEmailsByRef <- dataAccess.rawlsGroupQuery.loadEmails(projects.map(p => p.groups.values.map(g => g.subGroups)).flatten.flatten)
          operations <- dataAccess.rawlsBillingProjectQuery.loadOperationsForProjects(projects.map(_.projectName))
        } yield {
          (projects, groupEmailsByRef, operations)
        }
      }
      updatedProjectCount <- setupProjects(creatingProjects, groupEmailsByRef, operations)
    } yield {
      CheckDone(creatingProjects.size - updatedProjectCount)
    }

  }

  def setupProjects(projects: Seq[RawlsBillingProject],
                    groupEmailsByRef: Map[RawlsGroupRef, RawlsGroupEmail],
                    operations: Seq[RawlsBillingProjectOperationRecord])(implicit executionContext: ExecutionContext): Future[Int] = {

    for {
      updatedOperations <- Future.traverse(operations) { operation =>
        operation match {
          case RawlsBillingProjectOperationRecord(_, _, _, true, _, _) => Future.successful(operation)
          case RawlsBillingProjectOperationRecord(_, _, _, false, _, _) => gcsDAO.pollOperation(operation)
        }
      }

      // save the updates
      _ <- datasource.inTransaction { dataAccess =>
        val changedOperations = updatedOperations.toSet -- operations.toSet
        dataAccess.rawlsBillingProjectQuery.updateOperations(changedOperations.toSeq)
      }

      operationsByProject = updatedOperations.groupBy(rec => RawlsBillingProjectName(rec.projectName))

      maybeUpdatedProjects <- Future.traverse(projects) { project =>
        val nextStepFuture = operationsByProject(project.projectName) match {
          case Seq() =>
            // for some reason there are no operations, mark it as an error
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option("Internal error: no operations created")))

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.CREATE_PROJECT_OPERATION, _, true, None, _)) =>
            // create project operation finished successfully
            gcsDAO.beginProjectSetup(project, projectTemplate, groupEmailsByRef).flatMap {
              case util.Failure(t) =>
                logger.info(s"Failure creating project $project", t)
                Future.successful(project.copy(status = CreationStatuses.Error, message = Option(t.getMessage)))
              case Success(operations) => datasource.inTransaction { dataAccess =>
                dataAccess.rawlsBillingProjectQuery.insertOperations(operations).map(_ => project)
              }
            }

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.CREATE_PROJECT_OPERATION, _, true, Some(error), _)) =>
            // create project operation finished with an error
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(error)))

          case operations: Seq[RawlsBillingProjectOperationRecord] if operations.forall(rec => rec.done && rec.errorMessage.isEmpty) =>
            // all operations completed successfully
            gcsDAO.completeProjectSetup(project) map {
              case util.Failure(t) =>
                logger.info(s"Failure completing project for $project", t)
                project.copy(message = Option(t.getMessage))
              case Success(_) => project.copy(status = CreationStatuses.Ready)
            }

          case operations: Seq[RawlsBillingProjectOperationRecord] if operations.forall(rec => rec.done) =>
            // all operations completed but some failed
            val messages = operations.collect {
              case RawlsBillingProjectOperationRecord(_, operationName, _, true, Some(error), _) => s"Failure enabling api $operationName: ${error}"
            }
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(messages.mkString("[", "], [", "]"))))

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
