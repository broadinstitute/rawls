package org.broadinstitute.dsde.rawls.bucketMigration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamWorkspaceActions,
  Workspace,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration._
import org.broadinstitute.dsde.rawls.util.{RoleSupport, WorkspaceSupport}

import scala.concurrent.{ExecutionContext, Future}

class BucketMigrationService(val dataSource: SlickDataSource, val samDAO: SamDAO, val gcsDAO: GoogleServicesDAO)(
  val ctx: RawlsRequestContext
)(implicit val executionContext: ExecutionContext)
    extends RoleSupport
    with WorkspaceSupport
    with LazyLogging {

  /**
    * Helper functions to enforce appropriate authz and load workspace(s) if authz passes
    */
  private def asFCAdminWithWorkspace[T](workspaceName: WorkspaceName)(op: Workspace => Future[T]): Future[T] =
    asFCAdmin {
      for {
        workspace <- getV2WorkspaceContext(workspaceName)
        res <- op(workspace)
      } yield res
    }

  private def asOwnerWithWorkspace[T](workspaceName: WorkspaceName)(op: Workspace => Future[T]): Future[T] =
    for {
      workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own)
      res <- op(workspace)
    } yield res

  private def asFCAdminWithBillingProjectWorkspaces[T](
    billingProject: RawlsBillingProjectName
  )(op: Seq[Workspace] => Future[T]): Future[T] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction(_.workspaceQuery.listWithBillingProject(billingProject))
        res <- op(workspaces)
      } yield res
    }

  private def asOwnerWithBillingProjectWorkspaces[T](
    billingProject: RawlsBillingProjectName
  )(op: Seq[Workspace] => Future[T]): Future[T] =
    for {
      isOwner <- samDAO.userHasAction(SamResourceTypeNames.billingProject,
                                      billingProject.value,
                                      SamBillingProjectActions.own,
                                      ctx
      )
      _ = if (!isOwner)
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "you must be an owner"))

      workspaces <- dataSource.inTransaction(_.workspaceQuery.listWithBillingProject(billingProject))
      res <- op(workspaces)
    } yield res

  /**
    * Entrypoints that make necessary authz checks then call relevant method to do the actual work
    */
  def adminGetBucketMigrationProgressForWorkspace(
    workspaceName: WorkspaceName
  ): Future[Option[MultiregionalBucketMigrationProgress]] =
    asFCAdminWithWorkspace(workspaceName) {
      getBucketMigrationProgressForWorkspaceInternal
    }

  def getBucketMigrationProgressForWorkspace(
    workspaceName: WorkspaceName
  ): Future[Option[MultiregionalBucketMigrationProgress]] =
    asOwnerWithWorkspace(workspaceName) {
      getBucketMigrationProgressForWorkspaceInternal
    }

  def adminGetBucketMigrationProgressForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    asFCAdminWithBillingProjectWorkspaces(billingProjectName) {
      getBucketMigrationProgressForBillingProjectInternal
    }

  def getBucketMigrationProgressForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    asOwnerWithBillingProjectWorkspaces(billingProjectName) {
      getBucketMigrationProgressForBillingProjectInternal
    }
  def adminGetBucketMigrationAttemptsForWorkspace(
    workspaceName: WorkspaceName
  ): Future[List[MultiregionalBucketMigrationMetadata]] =
    asFCAdminWithWorkspace(workspaceName) {
      getBucketMigrationAttemptsForWorkspaceInternal
    }

  def getBucketMigrationAttemptsForWorkspace(
    workspaceName: WorkspaceName
  ): Future[List[MultiregionalBucketMigrationMetadata]] =
    asOwnerWithWorkspace(workspaceName) {
      getBucketMigrationAttemptsForWorkspaceInternal
    }

  def adminGetBucketMigrationAttemptsForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]] =
    asFCAdminWithBillingProjectWorkspaces(billingProjectName) {
      getBucketMigrationAttemptsForBillingProjectInternal
    }

  def getBucketMigrationAttemptsForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]] =
    asOwnerWithBillingProjectWorkspaces(billingProjectName) {
      getBucketMigrationAttemptsForBillingProjectInternal
    }

  def adminMigrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    asFCAdminWithWorkspace(workspaceName) {
      migrateWorkspaceBucketInternal
    }

  def migrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    asOwnerWithWorkspace(workspaceName) {
      migrateWorkspaceBucketInternal
    }

  def adminMigrateAllWorkspaceBuckets(
    workspaceNames: Iterable[WorkspaceName]
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction(_.workspaceQuery.listByNames(workspaceNames.toList))
        res <- migrateWorkspaceBuckets(workspaces)
      } yield res
    }

  def migrateAllWorkspaceBuckets(
    workspaceNames: Iterable[WorkspaceName]
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    for {
      workspaces <- dataSource.inTransaction(_.workspaceQuery.listByNames(workspaceNames.toList))
      ownsAllWorkspaces <- workspaces.traverse { workspace =>
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.own, ctx)
      }
      _ = if (!ownsAllWorkspaces.forall(identity))
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "you must own all workspaces"))
      res <- migrateWorkspaceBuckets(workspaces)
    } yield res

  def adminMigrateWorkspaceBucketsInBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdminWithBillingProjectWorkspaces(billingProjectName) {
      migrateWorkspaceBuckets
    }

  def migrateWorkspaceBucketsInBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asOwnerWithBillingProjectWorkspaces(billingProjectName) {
      migrateWorkspaceBuckets
    }

  /**
    * Shared methods to do the actual work
    */
  private def getBucketMigrationProgressForWorkspaceInternal(
    workspace: Workspace
  ): Future[Option[MultiregionalBucketMigrationProgress]] =
    dataSource.inTransaction(getBucketMigrationProgress(workspace))

  private def getBucketMigrationProgressForBillingProjectInternal(workspaces: Seq[Workspace]) =
    workspaces
      .traverse { workspace =>
        dataSource
          .inTransaction(getBucketMigrationProgress(workspace))
          .recover(_ => None)
          .map(workspace.toWorkspaceName.toString -> _)
      }
      .map(_.toMap)

  private def getBucketMigrationProgress(
    workspace: Workspace
  )(dataAccess: DataAccess): ReadWriteAction[Option[MultiregionalBucketMigrationProgress]] = {
    import dataAccess.driver.api._
    for {
      attemptOpt <- dataAccess.multiregionalBucketMigrationQuery
        .getAttempt(workspace.workspaceIdAsUUID)
        .value
      attempt = attemptOpt.getOrElse(
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound,
                      s"No past migration attempts found for workspace ${workspace.toWorkspaceName}"
          )
        )
      )

      tempTransferJob <-
        MultiregionalStorageTransferJobs.storageTransferJobs
          .filter(_.migrationId === attempt.id)
          .filter(_.sourceBucket === workspace.bucketName)
          .sortBy(_.created.desc)
          .result
          .headOption

      finalTransferJob <-
        MultiregionalStorageTransferJobs.storageTransferJobs
          .filter(_.migrationId === attempt.id)
          .filter(_.destBucket === workspace.bucketName)
          .sortBy(_.created.desc)
          .result
          .headOption

    } yield MultiregionalBucketMigrationProgress(
      MultiregionalBucketMigrationStep.fromMultiregionalBucketMigration(attempt),
      attempt.outcome,
      STSJobProgress.fromMultiregionalStorageTransferJob(tempTransferJob),
      STSJobProgress.fromMultiregionalStorageTransferJob(finalTransferJob)
    ).some
  }

  private def getBucketMigrationAttemptsForWorkspaceInternal(workspace: Workspace) =
    for {
      attempts <- dataSource.inTransaction { dataAccess =>
        dataAccess.multiregionalBucketMigrationQuery.getMigrationAttempts(workspace)
      }
    } yield attempts.mapWithIndex(MultiregionalBucketMigrationMetadata.fromMultiregionalBucketMigration)

  private def getBucketMigrationAttemptsForBillingProjectInternal(workspaces: Seq[Workspace]) =
    for {
      attempts <- workspaces.traverse { workspace =>
        dataSource
          .inTransaction { dataAccess =>
            dataAccess.multiregionalBucketMigrationQuery.getMigrationAttempts(workspace)
          }
          .map { attempts =>
            workspace.toWorkspaceName.toString -> attempts.mapWithIndex(
              MultiregionalBucketMigrationMetadata.fromMultiregionalBucketMigration
            )
          }
      }
    } yield attempts.toMap

  private def migrateWorkspaceBucketInternal(workspace: Workspace) =
    for {
      location <- getBucketLocation(workspace)

      metadata <- dataSource.inTransaction { dataAccess =>
        dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspace, location)
      }
    } yield metadata

  private def migrateWorkspaceBuckets(
    workspaces: Seq[Workspace]
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    for {
      workspacesWithBucketLocation <- workspaces.traverse(workspace => getBucketLocation(workspace).map(workspace -> _))
      migrationErrorsOrAttempts <- dataSource.inTransaction { dataAccess =>
        workspacesWithBucketLocation.traverse { case (workspace, location) =>
          MonadThrow[ReadWriteAction].attempt {
            dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspace, location)
          }
        }
      }
    } yield {
      val (errors, migrationAttempts) = migrationErrorsOrAttempts.partitionMap(identity)
      if (errors.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest,
                      "One or more workspace buckets could not be scheduled for migration",
                      errors.map(ErrorReport.apply)
          )
        )
      } else {
        migrationAttempts
      }
    }

  private def getBucketLocation(workspace: Workspace): Future[Option[String]] =
    gcsDAO.getBucket(workspace.bucketName, workspace.googleProjectId.some).map {
      case Left(_)       => None
      case Right(bucket) => Option(bucket.getLocation)
    }
}

object BucketMigrationService {
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(ctx: RawlsRequestContext)(
    implicit executionContext: ExecutionContext
  ) = new BucketMigrationService(dataSource, samDAO, gcsDAO)(ctx)
}
