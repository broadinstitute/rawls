package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import cats.effect.IO
import com.google.api.services.accesscontextmanager.v1.model.Operation
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.FutureSupport

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Success, Try}


object V1WorkspaceMigrationMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate, requesterPaysRole: String)(implicit executionContext: ExecutionContext): Props = {
    Props(new V1WorkspaceMigrationActor(datasource, gcsDAO, samDAO, projectTemplate, requesterPaysRole))
  }

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class V1WorkspaceMigrationActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO, val samDAO: SamDAO, val projectTemplate: ProjectTemplate, val requesterPaysRole: String)(implicit val executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
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
trait V1WorkspaceMigrationMonitor extends LazyLogging with FutureSupport {
  implicit val executionContext: ExecutionContext
  val datasource: SlickDataSource
  val workspaceService: WorkspaceService
  val gcsDAO: GoogleServicesDAO
  val projectTemplate: ProjectTemplate
  val samDAO: SamDAO
  val requesterPaysRole: String

  // todo: CA-1183 create second monitor to clean up old Google projects from fully migrated Terra billing projects

  def checkForMigratingWorkspaces(): Future[Unit] = { Future.unit }

  def getWorkspaceForMigration(): Some[(Workspace, RawlsBillingProject)]
  def createV2Workspace(billingProject: RawlsBillingProject, name: WorkspaceName): Some[(Workspace, RawlsBillingProject)]
  def lockWorkspace(v1Workspace: Workspace) = {
    // Should block new workflows
    // Should block new cloud environments

    // todo: abstract lock logic from business logic
    workspaceService.lockWorkspace(v1Workspace.toWorkspaceName)
  }

  def migrateWorkspaceBucket(v1Workspace: Workspace, googleProject: GoogleProject) = {
    // Create temp bucket
    // Copy Files to temp bucket
    // Delete original bucket
    // Create new bucket with same name
    // Copy files from temp bucket
  }

  def migrateCloudEnvironments(v1Workspace: Workspace, googleProject: GoogleProject) = {
    /* covers:
     apps, (known to be ephemeral, maybe we don't need to do?)
      dataproc clusters, (known to be ephemeral, maybe we don't need to do?)
       persistent disks (punting on this)
     */
  }

  def getGoogleProject(v1Workspace: Workspace, billingProject: RawlsBillingProject) = {
    // We could avoid doing this for 1-1 billingProject Workspace relationships (Punting)
    // claim project from rbs
    workspaceService.setupGoogleProject(
      billingProject,
      billingProject.billingAccount.getOrElse(throw new Exception),
      v1Workspace.workspaceId,
      v1Workspace.toWorkspaceName,
      policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail], // todo: get these from Sam
      billingProjectOwnerPolicyEmail: WorkbenchEmail,
      span: Span = null)
  }

  def assocWorkspaceWithGoogleProject(v1Workspace: Workspace, googleProject: GoogleProject) = {
    // Create sam resource and assoc with workspace
    // shuts down old project
  }

  def migrate(v1Workspace: Workspace, billingProject: RawlsBillingProject): IO[Unit] = {
    // Allocate destination workspace
    for {
      lock <- lockWorkspace(v1Workspace)
      googleProject = getGoogleProject(v1Workspace, billingProject)
      _ <- migrateWorkspaceBucket(v1Workspace, googleProject)
      _ <- migrateCloudEnvironments(v1Workspace, googleProject)
      _ <- assocWorkspaceWithGoogleProject(v1Workspace, googleProject)
      _ <- unlockWorkspace(lock)
      _ <- deleteBillingProjectGoogleProjectIfLastWorkspace()
    }
    IO[Unit]
  }
}
