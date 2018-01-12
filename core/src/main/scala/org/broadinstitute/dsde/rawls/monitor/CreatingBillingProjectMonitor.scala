package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor.{CheckDone, CheckNow}
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * Created by dvoet on 8/22/16.
 */
object CreatingBillingProjectMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, samDAO, projectTemplate))
  }

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
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
              ownerGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamProjectRoles.owner).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
              computeUserGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, UserService.canComputeUserPolicyName).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))

              updatedTemplate = projectTemplate.copy(policies = projectTemplate.policies ++ Map(
                "roles/viewer" -> Seq(s"group:${ownerGroupEmail.value}"),
                "roles/billing.projectManager" -> Seq(s"group:${ownerGroupEmail.value}"),
                "roles/genomics.pipelinesRunner" -> Seq(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}")))

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
            Future.successful(project.copy(status = CreationStatuses.Error, message = Option(error)))

          case operations: Seq[RawlsBillingProjectOperationRecord] if operations.forall(rec => rec.done && rec.errorMessage.isEmpty) =>
            // all operations completed successfully
            for {
              ownerGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamProjectRoles.owner).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
              computeUserGroupEmail <- samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, UserService.canComputeUserPolicyName).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))

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
