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
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, pubSubDAO: GooglePubSubDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate, requesterPaysRole: String, dmConfig: Config)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, pubSubDAO, samDAO, projectTemplate, requesterPaysRole, dmConfig))
  }

  //shiny new Deployment Manager flow
  sealed trait CreatingBillingProjectMonitorMessage
  case object Startup extends CreatingBillingProjectMonitorMessage
  case object CheckPubSub extends CreatingBillingProjectMonitorMessage

  //old, operation-based way of monitoring BPs
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val pubSubDAO: GooglePubSubDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate, val requesterPaysRole: String, val dmConfig: Config)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
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
  val dmConfig: Config

  val dmPubSubTopic = dmConfig.getString("pubSubTopic")
  val dmPubSubSubscription = dmConfig.getString("pubSubSubscription")
  val dmTemplatePath = dmConfig.getString("templatePath")
  val dmProject = dmConfig.getString("projectID")

  def startup()(implicit executionContext: ExecutionContext): Future[CreatingBillingProjectMonitorMessage] = {
    for {
      _ <- pubSubDAO.createTopic(dmPubSubTopic)
      _ <- pubSubDAO.createSubscription(dmPubSubTopic, dmPubSubSubscription)
    } yield {
      CheckPubSub
    }
  }

  def updateBillingProjects(projectMap: Map[RawlsBillingProjectName, GooglePubSubDAO.PubSubMessage]): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      //these messages will include info from all projects from all rawlses, not just this one.
      // get the BPs we know about in the db and iterate over them.
      dataAccess.rawlsBillingProjectQuery.getBillingProjects(projectMap.keySet) flatMap { projectsOwnedByMe =>
        val bpUpdates = projectsOwnedByMe map { project =>
          if (projectMap(project.projectName).attributes("status") == "ERROR") {
            //assuming that the error is showing up in the body of the message here.
            project.copy(status = CreationStatuses.Error, message = Some(projectMap(project.projectName).contents))
          } else {
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

  //TODO: bye. also all the oper stuff :)
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
      updatedOperations <- Future.traverse(operations) { operation =>
        operation match {
          case RawlsBillingProjectOperationRecord(_, _, _, true, _, _) => Future.successful(operation)
          case RawlsBillingProjectOperationRecord(_, _, _, false, _, _) => gcsDAO.pollOperation(operation).recover {
            // if we don't mark this as done we might continue retrying forever and pollOperation already does some retrying
            case t: Throwable => operation.copy(done = true, errorMessage = Option(s"error getting ${operation.operationName} operation status: ${t.getMessage}"))
          }
        }
      }

      // save the updates
      _ <- datasource.inTransaction { dataAccess =>
        val changedOperations = updatedOperations.toSet -- operations.toSet
        dataAccess.rawlsBillingProjectQuery.updateOperations(changedOperations.toSeq)
      }

      operationsByProject = updatedOperations.groupBy(rec => RawlsBillingProjectName(rec.projectName))

      maybeUpdatedProjects <- Future.traverse(projects) { project =>
        // this match figures out the current state of the project and progresses it to the next step when appropriate
        // see GoogleServicesDAO.createProject for more details
        val nextStepFuture = operationsByProject(project.projectName) match {
          case Seq() =>
            // there are no operations, there is a small window when this can happen but it should resolve itself so let it pass
            Future.successful(project)

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.CREATE_PROJECT_OPERATION, _, true, None, _)) =>
            // create project operation finished successfully
            for {
              ownerGroupEmail <- UserService.getGoogleProjectOwnerGroupEmail(samDAO, project.projectName)
              computeUserGroupEmail <- UserService.getComputeUserGroupEmail(samDAO, project.projectName)

              updatedTemplate = projectTemplate.copy(
                policies = projectTemplate.policies ++
                  UserService.getDefaultGoogleProjectPolicies(ownerGroupEmail, computeUserGroupEmail, requesterPaysRole))

              updatedProject <- gcsDAO.beginProjectSetup(project, updatedTemplate).flatMap {
                case util.Failure(t) =>
                  logger.info(s"Failure creating project $project", t)
                  Future.successful(project.copy(status = CreationStatuses.Error, message = Option(t.getMessage)))
                case Success(operationRecords) => datasource.inTransaction { dataAccess =>
                  dataAccess.rawlsBillingProjectQuery.insertOperations(operationRecords).map(_ => project)
                }
              }
            } yield {
              updatedProject
            }

          case Seq(RawlsBillingProjectOperationRecord(_, gcsDAO.CREATE_PROJECT_OPERATION, _, true, Some(error), _)) =>
            // create project operation finished with an error
            logger.debug(s"project ${project.projectName} creation finished with errors: $error")
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(error)))

          case operations: Seq[RawlsBillingProjectOperationRecord] if operations.forall(rec => rec.done && rec.errorMessage.isEmpty) =>
            // all operations completed successfully
            for {
              ownerGroupEmail <- UserService.getGoogleProjectOwnerGroupEmail(samDAO, project.projectName)
              computeUserGroupEmail <- UserService.getComputeUserGroupEmail(samDAO, project.projectName)
              updatedProject <- gcsDAO.completeProjectSetup(project, Set(ownerGroupEmail, computeUserGroupEmail)) map {
                case util.Failure(t) =>
                  logger.info(s"Failure completing project for $project", t)
                  project.copy(message = Option(t.getMessage))
                case Success(_) => project.copy(status = CreationStatuses.Ready)
              }
            } yield {
              updatedProject
            }

          case operations: Seq[RawlsBillingProjectOperationRecord] if operations.forall(rec => rec.done) =>
            // all operations completed but some failed
            val messages = operations.collect {
              case RawlsBillingProjectOperationRecord(_, operationName, _, true, Some(error), _) => s"Failure enabling api $operationName: ${error}"
            }
            val errorMessage = messages.mkString("[", "], [", "]")
            logger.debug(s"errors enabling apis for project ${project.projectName}: $errorMessage")
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(errorMessage)))

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
