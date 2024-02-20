package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigration
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.migration._

import scala.concurrent.{ExecutionContext, Future}

class DisabledBucketMigrationService()(val ctx: RawlsRequestContext)(implicit val executionContext: ExecutionContext)
  extends BucketMigration {
  def getEligibleOrMigratingWorkspaces: Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    throw new NotImplementedError("getEligibleOrMigratingWorkspaces is not implemented for Azure.")
  def adminGetBucketMigrationProgressForWorkspace(workspaceName: WorkspaceName): Future[Option[MultiregionalBucketMigrationProgress]] =
    throw new NotImplementedError("adminGetBucketMigrationProgressForWorkspace is not implemented for Azure.")
  def getBucketMigrationProgressForWorkspace(workspaceName: WorkspaceName): Future[Option[MultiregionalBucketMigrationProgress]] =
    throw new NotImplementedError("getBucketMigrationProgressForWorkspace is not implemented for Azure.")
  def adminGetBucketMigrationProgressForWorkspaces(workspaces: Iterable[WorkspaceName]): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    throw new NotImplementedError("adminGetBucketMigrationProgressForWorkspaces is not implemented for Azure.")
  def getBucketMigrationProgressForWorkspaces(workspaces: Iterable[WorkspaceName]): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    throw new NotImplementedError("getBucketMigrationProgressForWorkspaces is not implemented for Azure.")
  def adminGetBucketMigrationProgressForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    throw new NotImplementedError("adminGetBucketMigrationProgressForBillingProject is not implemented for Azure.")
  def getBucketMigrationProgressForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]] =
    throw new NotImplementedError("getBucketMigrationProgressForBillingProject is not implemented for Azure.")
  def adminGetBucketMigrationAttemptsForWorkspace(workspaceName: WorkspaceName): Future[List[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("adminGetBucketMigrationAttemptsForWorkspace is not implemented for Azure.")
  def getBucketMigrationAttemptsForWorkspace(workspaceName: WorkspaceName): Future[List[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("getBucketMigrationAttemptsForWorkspace is not implemented for Azure.")
  def adminGetBucketMigrationAttemptsForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]] =
    throw new NotImplementedError("adminGetBucketMigrationAttemptsForBillingProject is not implemented for Azure.")
  def getBucketMigrationAttemptsForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]] =
    throw new NotImplementedError("getBucketMigrationAttemptsForBillingProject is not implemented for Azure.")
  def adminMigrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    throw new NotImplementedError("adminMigrateWorkspaceBucket is not implemented for Azure.")
  def migrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata] =
    throw new NotImplementedError("migrateWorkspaceBucket is not implemented for Azure.")
  def adminMigrateAllWorkspaceBuckets(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("adminMigrateAllWorkspaceBuckets is not implemented for Azure.")
  def migrateAllWorkspaceBuckets(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("migrateAllWorkspaceBuckets is not implemented for Azure.")
  def adminMigrateWorkspaceBucketsInBillingProject(billingProjectName: RawlsBillingProjectName): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("adminMigrateWorkspaceBucketsInBillingProject is not implemented for Azure.")
  def migrateWorkspaceBucketsInBillingProject(billingProjectName: RawlsBillingProjectName): Future[Iterable[MultiregionalBucketMigrationMetadata]] =
    throw new NotImplementedError("migrateWorkspaceBucketsInBillingProject is not implemented for Azure.")
}

object DisabledBucketMigrationService {
  def constructor()(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext
  ) = new DisabledBucketMigrationService()(ctx)
}