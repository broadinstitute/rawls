package org.broadinstitute.dsde.rawls.bucketMigration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsBillingProjectName, RawlsRequestContext, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.monadThrowDBIOAction
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationMetadata
import org.broadinstitute.dsde.rawls.util.{RoleSupport, WorkspaceSupport}

import scala.concurrent.{ExecutionContext, Future}

class BucketMigrationService(val dataSource: SlickDataSource, val samDAO: SamDAO, val gcsDAO: GoogleServicesDAO)(
  val ctx: RawlsRequestContext
)(implicit val executionContext: ExecutionContext)
    extends RoleSupport
    with WorkspaceSupport
    with LazyLogging {

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
              workspace.name -> attempts.mapWithIndex(
                MultiregionalBucketMigrationMetadata.fromMultiregionalBucketMigration
              )
            }
        }
      } yield attempts.toMap
    }

  def migrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    asFCAdmin {
      logger.info(s"Scheduling Workspace '$workspaceName' for bucket migration")
      dataSource.inTransaction { dataAccess =>
        dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspaceName)
      }
    }

  def migrateAllWorkspaceBuckets(
    workspaceNames: Iterable[WorkspaceName]
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      dataSource
        .inTransaction { dataAccess =>
          for {
            errorsOrMigrationAttempts <- workspaceNames.toList.traverse { workspaceName =>
              MonadThrow[ReadWriteAction].attempt {
                dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspaceName)
              }
            }
          } yield errorsOrMigrationAttempts
        }
        .map(raiseFailedMigrationAttempts)
    }

  def migrateWorkspaceBucketsInBillingProject(
    billingProjectName: RawlsBillingProjectName
  ): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    asFCAdmin {
      dataSource
        .inTransaction { dataAccess =>
          for {
            workspaces <- dataAccess.workspaceQuery.listWithBillingProject(billingProjectName)
            errorsOrMigrationAttempts <- workspaces.traverse { workspace =>
              MonadThrow[ReadWriteAction].attempt {
                dataAccess.multiregionalBucketMigrationQuery.scheduleAndGetMetadata(workspace.toWorkspaceName)
              }
            }
          } yield errorsOrMigrationAttempts
        }
        .map(raiseFailedMigrationAttempts)
    }

  private def raiseFailedMigrationAttempts(
    errorsOrMigrationAttempts: Seq[Either[Throwable, MultiregionalBucketMigrationMetadata]]
  ): Seq[MultiregionalBucketMigrationMetadata] = {
    val (errors, migrationAttempts) = errorsOrMigrationAttempts.partitionMap(identity)
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
}

object BucketMigrationService {
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(ctx: RawlsRequestContext)(
    implicit executionContext: ExecutionContext
  ) = new BucketMigrationService(dataSource, samDAO, gcsDAO)(ctx)
}
