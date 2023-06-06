package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import cats.implicits.{catsSyntaxOptionId, toFoldableOps}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor._
import org.broadinstitute.dsde.workbench.util.FutureSupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
 * Created by dvoet on 8/22/16.
 */
object CreatingBillingProjectMonitor {
  def props(datasource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            projectTemplate: ProjectTemplate,
            requesterPaysRole: String
  )(implicit executionContext: ExecutionContext): Props =
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO, samDAO, projectTemplate, requesterPaysRole))

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource,
                                         val gcsDAO: GoogleServicesDAO,
                                         val samDAO: SamDAO,
                                         val projectTemplate: ProjectTemplate,
                                         val requesterPaysRole: String
)(implicit val executionContext: ExecutionContext)
    extends Actor
    with CreatingBillingProjectMonitor
    with LazyLogging {
  self ! CheckNow

  override def receive = {
    case CheckNow => checkCreatingProjects pipeTo self

    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.  However, if projects are being created, we want to poll more frequently, say ~once per 5 seconds.
    case CheckDone(creatingCount) if creatingCount > 0 =>
      context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)
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
trait CreatingBillingProjectMonitor extends LazyLogging with FutureSupport {
  implicit val executionContext: ExecutionContext
  val datasource: SlickDataSource
  val gcsDAO: GoogleServicesDAO
  val projectTemplate: ProjectTemplate
  val samDAO: SamDAO
  val requesterPaysRole: String

  /**
    * When creating projects, we call a set of "operations" that handle all the steps we need to run in order to: create
    * the project, enable APIs, etc., and get it into a valid state so that it can be
    * used by Firecloud/Terra.  We use Deployment Manager to handle the bulk of the steps needed to create and set up
    * projects.  As of Project Per Workspace (PPW), workspaces have their own google projects so any new v1 billing
    * project will not get their google project added to the service perimeter.
    * @return
    */
  def checkCreatingProjects(): Future[CheckDone] =
    for {
      (projectsBeingCreated, createProjectOperations) <- datasource.inTransaction { dataAccess =>
        for {
          projectsBeingCreated <- dataAccess.rawlsBillingProjectQuery.listProjectsWithCreationStatus(
            CreationStatuses.Creating
          )
          createProjectOperations <- dataAccess.rawlsBillingProjectQuery.loadOperationsForProjects(
            projectsBeingCreated.map(_.projectName),
            GoogleOperationNames.DeploymentManagerCreateProject
          )
        } yield (projectsBeingCreated, createProjectOperations)
      }
      latestCreateProjectOperations <- updateOperationRecordsFromGoogle(createProjectOperations)
      _ <- updateProjectsFromOperations(projectsBeingCreated, latestCreateProjectOperations)
    } yield CheckDone(projectsBeingCreated.size)

  /**
    * This method ensures that the state of the RawlsBillingProjectRecord matches the state of the
    * RawlsBillingProjectOperationRecord.  It takes a collection of RawlsBillingProjectOperationRecords that have
    * already been updated with the current state of the operations from Google.  This method will then update the
    * corresponding RawlsBillingProjectRecords if the Google operation has finished, and based on whether the operation
    * succeeded or failed.
    *
    * @param projects: collection of RawlsBillingProjects that we want to update
    * @param operations: collection of RawlsBillingProjectOperationRecords that reflect the latest operation information
    *                  from Google
    * @return an Int representing the number of RawlsBillingProjectRecords that were updated by running this method
    */
  private def updateProjectsFromOperations(projects: Seq[RawlsBillingProject],
                                           operations: Seq[RawlsBillingProjectOperationRecord]
  ): Future[Unit] = {
    val operationsByProject = operations.groupBy(rec => RawlsBillingProjectName(rec.projectName))
    projects.toList.traverse_ { project =>
      // figure out if the project operation is done yet and set the project status accordingly
      operationsByProject.get(project.projectName) match {
        case Some(Seq(RawlsBillingProjectOperationRecord(_, _, _, true, None, _))) =>
          // project operation finished successfully
          onSuccessfulProjectCreate(project)

        case Some(Seq(RawlsBillingProjectOperationRecord(_, _, _, true, Some(error), _))) =>
          // project operation finished with an error
          onFailedProjectCreate(project, error)

        case Some(tooManyOperations) if tooManyOperations.size > 1 =>
          // this should be impossible due to database constraints (duplicate primary keys)
          onFailedProjectCreate(project, s"Only expected one Operation, found $tooManyOperations")

        case _ =>
          // still running
          Future.unit
      }
    }
  }

  /**
    * Takes a collection of RawlsBillingProjectOperationRecords, checks on the status of all of the corresponding
    * operations in Google, updates and persists any updates to the the RawlsBillingProjectOperationRecords, and returns
    * the a copy of the RawlsBillingProjectOperationRecords updated with latest status from Google.
    * @param operations
    * @return
    */
  private def updateOperationRecordsFromGoogle(
    operations: Seq[RawlsBillingProjectOperationRecord]
  ): Future[Seq[RawlsBillingProjectOperationRecord]] =
    for {
      updatedOperations <- checkOperationsInGoogle(operations)

      // save only the operation records that have changed
      _ <- datasource.inTransaction { dataAccess =>
        val changedOperations = updatedOperations.toSet -- operations.toSet
        dataAccess.rawlsBillingProjectQuery.updateOperations(changedOperations.toSeq)
      }
    } yield updatedOperations

  /**
    * Takes a collection of RawlsBillingProjectOperationRecords and checks with Google to see if the corresponding
    * operations in google have completed.  Returns a collection of RawlsBillingProjectOperationRecords updated to with
    * the latest operation information from Google.  This method does NOT persist those changes in the Rawls DB.
    * @param operations
    * @return
    */
  private def checkOperationsInGoogle(
    operations: Seq[RawlsBillingProjectOperationRecord]
  ): Future[Seq[RawlsBillingProjectOperationRecord]] = {
    // Collect the operationIds that we need to check.  There's a possibility that multiple operation records exist for
    // the same Google Operation, so we de-dupe to reduce volume of requests to Google.
    val operationsToPoll = operations.collect {
      case operation if !operation.done => OperationId(operation.api, operation.operationId)
    }.toSet

    for {
      // poll Google to get the latest status of the operations
      pollingResults <- Future.traverse(operationsToPoll) { case operationId =>
        gcsDAO
          .pollOperation(operationId)
          .recover {
            // if we don't mark this as done we might continue retrying forever and pollOperation already does some retrying
            case t: Throwable =>
              OperationStatus(
                true,
                Option(
                  s"error getting operation id ${operationId.operationId} from API ${operationId.apiType}. operation status: ${t.getMessage}"
                )
              )
          }
          .map(operationId -> _)
      }
      pollingResultsById = pollingResults.toMap
    } yield
    // Update RawlsBillingProjectOperationRecords in memory with the latest polling results
    operations.map { rawlsOperation =>
      pollingResultsById.get(OperationId(rawlsOperation.api, rawlsOperation.operationId)) match {
        case None => rawlsOperation
        case Some(googleOperationStatus) =>
          rawlsOperation.copy(done = googleOperationStatus.done, errorMessage = googleOperationStatus.errorMessage)
      }
    }
  }

  private def onSuccessfulProjectCreate(project: RawlsBillingProject): Future[Unit] =
    for {
      _ <- gcsDAO.cleanupDMProject(project.googleProjectId)
      googleProject <- gcsDAO.getGoogleProject(project.googleProjectId)
      _ <- datasource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.rawlsBillingProjectQuery.updateCreationStatus(project.projectName, CreationStatuses.Ready)
          _ <- dataAccess.rawlsBillingProjectQuery.updateGoogleProjectNumber(
            project.projectName,
            GoogleProjectNumber(googleProject.getProjectNumber.toString).some
          )
        } yield ()
      }
    } yield ()

  private def onFailedProjectCreate(project: RawlsBillingProject, error: String): Future[Unit] = {
    logger.debug(s"project ${project.projectName.value} creation finished with errors: $error")
    for {
      _ <- gcsDAO.cleanupDMProject(project.googleProjectId)
      _ <- datasource.inTransaction {
        _.rawlsBillingProjectQuery.updateCreationStatus(
          project.projectName,
          CreationStatuses.Error,
          s"project ${project.projectName.value} creation finished with errors: $error".some
        )
      }
    } yield ()
  }

}
