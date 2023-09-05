package org.broadinstitute.dsde.rawls.bucketMigration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext,
  Workspace,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration._
import org.broadinstitute.dsde.rawls.util.{RoleSupport, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}

import scala.concurrent.{ExecutionContext, Future}

class BucketMigrationService(val dataSource: SlickDataSource, val samDAO: SamDAO, val gcsDAO: GoogleServicesDAO)(
  val ctx: RawlsRequestContext
)(implicit val executionContext: ExecutionContext)
    extends RoleSupport
    with WorkspaceSupport
    with LazyLogging {

  def getBucketMigrationProgressForWorkspace(
    workspaceName: WorkspaceName
  ): Future[Option[MultiregionalBucketMigrationProgress]] =
    asFCAdmin {
      for {
        workspace <- getV2WorkspaceContext(workspaceName)
        res <- dataSource.inTransaction(getBucketMigrationProgress(workspace))
      } yield res
    }

  def getBucketMigrationProgressForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
        }
        progress <- workspaces.traverse { workspace =>
          dataSource
            .inTransaction(getBucketMigrationProgress(workspace))
            .recover(_ => None)
            .map(workspace.toWorkspaceName.toString -> _)
        }
      } yield progress.toMap
    }

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

  def getBucketMigrationAttemptsForWorkspace(
    workspaceName: WorkspaceName
  ): Future[List[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      for {
        workspace <- getV2WorkspaceContext(workspaceName)
        attempts <- dataSource.inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.getMigrationAttempts(workspace)
        }
      } yield attempts.mapWithIndex(MultiregionalBucketMigrationMetadata.fromMultiregionalBucketMigration)
    }

  def getBucketMigrationAttemptsForBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
        }
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
    }

  def migrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    asFCAdmin {
      logger.info(s"Scheduling Workspace '$workspaceName' for bucket migration")
      for {
        workspaceOpt <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.findByName(workspaceName)
        }
        workspace = workspaceOpt.getOrElse(throw new NoSuchWorkspaceException(workspaceName.toString))
        location <- getBucketLocation(workspace)

        metadata <- dataSource.inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspace, location)
        }
      } yield metadata
    }

  def migrateAllWorkspaceBuckets(
    workspaceNames: Iterable[WorkspaceName]
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listByNames(workspaceNames.toList)
        }

        res <- migrateWorkspaces(workspaces.toList)
      } yield res
    }

  def migrateWorkspaceBucketsInBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
        }

        res <- migrateWorkspaces(workspaces.toList)
      } yield res
    }

  private def migrateWorkspaces(workspaces: List[Workspace]): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
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
