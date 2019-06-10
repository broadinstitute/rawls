package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

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

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate, val requesterPaysRole: String)(implicit val executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
  self ! CheckNow

  override def receive = {
    case CheckNow => checkCreatingProjects pipeTo self

    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.  However, if projects are being created, we want to poll more frequently, say ~once per 5 seconds.
    case CheckDone(creatingCount) if creatingCount > 0 => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)
    case CheckDone(creatingCount) => context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring creating billing projects", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
  }
}

/**
  * This monitor ensures that we create projects that are usable by Firecloud/Terra.  To do this, we have a
  * "CreationStatus" on RawlsBillingProject instances that keeps track of what state the project is in and whether it is
  * still being created/setup and whether it is done or in some kind of error state.  To keep track of all of this, this
  * class's responsibility is to create and update RawlsBillingProjectOperationRecords in Rawls, trigger operations in
  * Google, and keep RawlsBillingProject records up to date with what is actually created/ready/done in Google.
  */
trait CreatingBillingProjectMonitor extends LazyLogging {
  implicit val executionContext: ExecutionContext
  val datasource: SlickDataSource
  val gcsDAO: GoogleServicesDAO
  val projectTemplate: ProjectTemplate
  val samDAO: SamDAO
  val requesterPaysRole: String

  /**
    * When creating projects, we call a set of "operations" that handle all the steps we need to run in order to: create
    * the project, enable APIs, add it to a VPC-SC Security Perimeter, etc., and get it into a valid state so that it can be
    * used by Firecloud/Terra.  We use Deployment Manager to handle the bulk of the steps needed to create and set up
    * projects.  If a project needs to be added to a VPC-SC perimeter, this task is handled as a follow-up operation
    * that we can only run AFTER a project is created.
    * @return
    */
  def checkCreatingProjects(): Future[CheckDone] = {
    for {
      (projectsBeingCreated, createProjectOperations, projectsBeingAddedToPerimeter, addProjectToPerimeterOperations) <- datasource.inTransaction { dataAccess =>
        for {
          projectsBeingCreated <- dataAccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(CreationStatuses.Creating)
          projectsBeingAddedToPerimeter <- dataAccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(CreationStatuses.AddingToPerimeter)
          createProjectOperations <- dataAccess.rawlsBillingProjectQuery.loadOperationsForProjects(projectsBeingCreated.map(_.projectName), GoogleOperationNames.DeploymentManagerCreateProject)
          addProjectToPerimeterOperations <- dataAccess.rawlsBillingProjectQuery.loadOperationsForProjects(projectsBeingAddedToPerimeter.map(_.projectName), GoogleOperationNames.AddProjectToPerimeter)
        } yield {
          (projectsBeingCreated, createProjectOperations, projectsBeingAddedToPerimeter, addProjectToPerimeterOperations)
        }
      }
      latestCreateProjectOperations <- updateOperationRecordsFromGoogle(createProjectOperations)
      _ <- updateProjectsFromOperations(projectsBeingCreated, latestCreateProjectOperations, onSuccessfulProjectCreate, onFailedProjectCreate)
      latestAddProjectToPerimeterOperations <- updateOperationRecordsFromGoogle(addProjectToPerimeterOperations)
      _ <- updateProjectsFromOperations(projectsBeingAddedToPerimeter, latestAddProjectToPerimeterOperations, onSuccessfulAddProjectToPerimeter, onFailedAddProjectToPerimeter)
      _ <- addProjectsToPerimeter(projectsBeingAddedToPerimeter, latestAddProjectToPerimeterOperations)
    } yield {
      CheckDone(projectsBeingCreated.size + projectsBeingAddedToPerimeter.size)
    }
  }

  /**
    * Takes a collection of project records that we know have a status indicating they need to be added to a
    * perimeter - operations might already exist to add some of these projects to the perimeter.
    * The operations parameter contains all preexisting operations.  We then
    * compare the list of projects that need to be added to a perimeter to the list of operations already in progress
    * to add projects to a perimeter.  For each perimeter that needs one or more projects to be added to it, we will
    * kick off one operation to add all of those projects at the same time.
    * @param projects: Collection of RawlsBillingProjects that need to be added to a perimeter
    * @param operations: Collection of RawlsBillingProjectOperationRecord that are already running to add a project to
    *                  the specified perimeter
    * @return
    */
  private def addProjectsToPerimeter(projects: Seq[RawlsBillingProject], operations: Seq[RawlsBillingProjectOperationRecord]): Future[Unit] = {
    val projectsWithoutOperations = projects.filterNot(project => operations.exists(_.projectName == project.projectName.value))
    if (projectsWithoutOperations.nonEmpty) {
      Future.traverse(projectsWithoutOperations.groupBy(_.servicePerimeter)) {
        case (None, projectsMissingPerimeter) => datasource.inTransaction { dataAccess =>
          val errorProjects = projectsMissingPerimeter.map { project =>
            project.copy(status = CreationStatuses.Error, message = Some("Failed to add project to perimeter because no perimeter specified"))
          }
          dataAccess.rawlsBillingProjectQuery.updateBillingProjects(errorProjects)
        }
        case (Some(servicePerimeterName: ServicePerimeterName), newProjectsInPerimeter) =>
          createAddProjectsToPerimeterOperation(servicePerimeterName, newProjectsInPerimeter)
      }.map(_ => ())
    } else {
      Future.successful(())
    }
  }

  /**
    * Takes the name of a VPC-SC Service Perimeter that needs to be updated along with a collection of
    * RawlsBillingProjects that need to be added to the perimeter.  Caution: the Google APIs for PATCHing a Service
    * Perimeter will OVERWRITE the project list with the provided list.  Therefore, whenever we update the list of
    * projects in a perimeter, we must specify the ENTIRE membership list.  We can get the intended membership list from
    * the Rawls DB.
    * @param servicePerimeterName
    * @param newProjectsInPerimeter all of these projects must be in servicePerimeterName
    * @throws IllegalArgumentException if there are any members of newProjectsInPerimeter not in servicePerimeterName
    * @return
    */
  private def createAddProjectsToPerimeterOperation(servicePerimeterName: ServicePerimeterName, newProjectsInPerimeter: Seq[RawlsBillingProject]): Future[Unit] = {
    if (newProjectsInPerimeter.exists(_.servicePerimeter != Some(servicePerimeterName))) {
      Future.failed(new IllegalArgumentException(s"all members of newProjectsInPerimeter must be in servicePerimeter $servicePerimeterName"))
    } else {
      for {
        // Query Rawls DB to get full list of projects intended to be inside the perimeter
        allProjectsInPerimeter <- datasource.inTransaction { dataAccess =>
          dataAccess.rawlsBillingProjectQuery.listProjectsWithServicePerimeter(servicePerimeterName)
        }
        allProjectNumbers = allProjectsInPerimeter.flatMap { project =>
          project.googleProjectNumber match {
            case Some(googleProjectNumber) => Option(googleProjectNumber.value)
            case None if !newProjectsInPerimeter.map(_.projectName).contains(project.projectName) =>
              // this case for a preexisting project that should already be in the perimeter but somehow does not
              // have a project id at this time. If this code is allowed to continue this project will be removed
              // from the perimeter which could allow data exfiltration which is BAD. So throw an exception which
              // will halt all perimeter operations for this perimeter and hope someone will notice and fix by
              // looking up the project number in google and adding it back to the database
              throw new RawlsException(s"project ${project.projectName} has a perimeter but does not have a project number in the database, please lookup the project in google console and add it to the database. Projects cannot be added to perimeter ${servicePerimeterName.value} until this is fixed")
          }
        }
        // Initiate operation to overwrite the list of projects in the Perimeter on Google
        operation <- gcsDAO.accessContextManagerDAO.overwriteProjectsInServicePerimeter(servicePerimeterName, allProjectNumbers)

        _ <- datasource.inTransaction { dataAccess =>
        val (projectsWithProjectNumber, projectsWithoutGoogleProjectNumber) = newProjectsInPerimeter.partition(_.googleProjectNumber.isDefined)
        for {
          _ <- dataAccess.rawlsBillingProjectQuery.insertOperations(projectsWithProjectNumber.map { project =>
            RawlsBillingProjectOperationRecord(project.projectName.value, GoogleOperationNames.AddProjectToPerimeter, operation.getName, false, None, GoogleApiTypes.AccessContextManagerApi)
          })
          _ <- dataAccess.rawlsBillingProjectQuery.updateBillingProjects(projectsWithoutGoogleProjectNumber.map { project =>
            project.copy(status = CreationStatuses.Error, message = Some("Project was in Adding to Perimeter state but google project number did not exist"))
          })
        } yield ()
        }
      } yield ()
    }
  }

  /**
    * This method ensures that the state of the RawlsBillingProjectRecord matches the state of the
    * RawlsBillingProjectOperationRecord.  It takes a collection of RawlsBillingProjectOperationRecords that have
    * already been updated with the current state of the operations from Google.  This method will then update the
    * corresponding RawlsBillingProjectRecords if the Google operation has finished, and based on whether the operation
    * succeeded or failed.
    * @param projects: collection of RawlsBillingProjects that we want to update
    * @param operations: collection of RawlsBillingProjectOperationRecords that reflect the latest operation information
    *                  from Google
    * @param onOperationSuccess: The function we want to run when an operation finished successfully on Google
    * @param onOperationFailure: The function we want to run when an operation finished with an error on Google
    * @return: an Int representing the number of RawlsBillingProjectRecords that were updated by running this method
    */
  private def updateProjectsFromOperations(projects: Seq[RawlsBillingProject], operations: Seq[RawlsBillingProjectOperationRecord], onOperationSuccess: RawlsBillingProject => Future[RawlsBillingProject], onOperationFailure: (RawlsBillingProject, String) => Future[RawlsBillingProject]): Future[Int] = {
    val operationsByProject = operations.groupBy(rec => RawlsBillingProjectName(rec.projectName))
    for {
      // Update project record based on the result of its Google operation
      maybeUpdatedProjects <- Future.traverse(projects) { project =>
        // figure out if the project operation is done yet and set the project status accordingly
        val nextStepFuture = operationsByProject.get(project.projectName) match {
          case Some(Seq(RawlsBillingProjectOperationRecord(_, _, _, true, None, _))) =>
            // project operation finished successfully
            onOperationSuccess(project)

          case Some(Seq(RawlsBillingProjectOperationRecord(_, _, _, true, Some(error), _))) =>
            // project operation finished with an error
            onOperationFailure(project, error)

          case Some(tooManyOperations) if tooManyOperations.size > 1 =>
            // this should be impossible due to database constraints (duplicate primary keys)
            onOperationFailure(project, s"Only expected one Operation, found $tooManyOperations")

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

      // Save only the project records that were changed
      _ <- datasource.inTransaction { dataAccess =>
        val updatedProjects = maybeUpdatedProjects.toSet -- projects.toSet
        dataAccess.rawlsBillingProjectQuery.updateBillingProjects(updatedProjects)
      }
    } yield {
      maybeUpdatedProjects.count(project => CreationStatuses.terminal.contains(project.status))
    }
  }

  /**
    * Takes a collection of RawlsBillingProjectOperationRecords, checks on the status of all of the corresponding
    * operations in Google, updates and persists any updates to the the RawlsBillingProjectOperationRecords, and returns
    * the a copy of the RawlsBillingProjectOperationRecords updated with latest status from Google.
    * @param operations
    * @return
    */
  private def updateOperationRecordsFromGoogle(operations: Seq[RawlsBillingProjectOperationRecord]): Future[Seq[RawlsBillingProjectOperationRecord]] = {
    for {
      updatedOperations <- checkOperationsInGoogle(operations)

      // save only the operation records that have changed
      _ <- datasource.inTransaction { dataAccess =>
        val changedOperations = updatedOperations.toSet -- operations.toSet
        dataAccess.rawlsBillingProjectQuery.updateOperations(changedOperations.toSeq)
      }
    } yield updatedOperations
  }

  /**
    * Takes a collection of RawlsBillingProjectOperationRecords and checks with Google to see if the corresponding
    * operations in google have completed.  Returns a collection of RawlsBillingProjectOperationRecords updated to with
    * the latest operation information from Google.  This method does NOT persist those changes in the Rawls DB.
    * @param operations
    * @return
    */
  private def checkOperationsInGoogle(operations: Seq[RawlsBillingProjectOperationRecord]): Future[Seq[RawlsBillingProjectOperationRecord]] = {
    // Collect the operationIds that we need to check.  There's a possibility that multiple operation records exist for
    // the same Google Operation, so we de-dupe to reduce volume of requests to Google.
    val operationsToPoll = operations.collect {
      case operation if !operation.done => OperationId(operation.api, operation.operationId)
    }.toSet

    for {
      // poll Google to get the latest status of the operations
      pollingResults <- Future.traverse(operationsToPoll) {
        case operationId => gcsDAO.pollOperation(operationId).recover {
          // if we don't mark this as done we might continue retrying forever and pollOperation already does some retrying
          case t: Throwable => OperationStatus(true, Option(s"error getting operation id ${operationId.operationId} from API ${operationId.apiType}. operation status: ${t.getMessage}"))
        }.map(operationId -> _)
      }
      pollingResultsById = pollingResults.toMap
    } yield {
      // Update RawlsBillingProjectOperationRecords in memory with the latest polling results
      operations.map { rawlsOperation =>
        pollingResultsById.get(OperationId(rawlsOperation.api, rawlsOperation.operationId)) match {
          case None => rawlsOperation
          case Some(googleOperationStatus) => rawlsOperation.copy(done = googleOperationStatus.done, errorMessage = googleOperationStatus.errorMessage)
        }
      }
    }
  }

  private def onSuccessfulProjectCreate(project: RawlsBillingProject): Future[RawlsBillingProject] = {
    for {
      _ <- gcsDAO.cleanupDMProject(project.projectName)
      googleProject <- gcsDAO.getGoogleProject(project.projectName)
    } yield {
      val status = project.servicePerimeter match {
        case Some(_) => CreationStatuses.AddingToPerimeter
        case None => CreationStatuses.Ready
      }
      project.copy(status = status, googleProjectNumber = Option(GoogleProjectNumber(googleProject.getProjectNumber.toString)))
    }
  }

  private def onFailedProjectCreate(project: RawlsBillingProject, error: String): Future[RawlsBillingProject] = {
    logger.debug(s"project ${project.projectName.value} creation finished with errors: $error")
    gcsDAO.cleanupDMProject(project.projectName) map { _ =>
      project.copy(status = CreationStatuses.Error, message = Option(s"project ${project.projectName.value} creation finished with errors: $error"))
    }
  }

  private def onSuccessfulAddProjectToPerimeter(project: RawlsBillingProject): Future[RawlsBillingProject] = {
    Future.successful(project.copy(status = CreationStatuses.Ready))
  }

  private def onFailedAddProjectToPerimeter(project: RawlsBillingProject, error: String): Future[RawlsBillingProject] = {
    logger.debug(s"project $project failed to add to perimeter with error message: $error")
    Future.successful(project.copy(status = CreationStatuses.Error, message = Option(s"Failed to add project to perimeter. Error message: $error")))
  }
}
