package org.broadinstitute.dsde.rawls.bucketMigration

import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, Workspace, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.migration._
import scala.concurrent.Future

trait BucketMigration{
  def getEligibleOrMigratingWorkspaces: Future[Map[String, Option[MultiregionalBucketMigrationProgress]]]
  def adminGetBucketMigrationProgressForWorkspace(workspaceName: WorkspaceName): Future[Option[MultiregionalBucketMigrationProgress]]
  def getBucketMigrationProgressForWorkspace(workspaceName: WorkspaceName): Future[Option[MultiregionalBucketMigrationProgress]]
  def adminGetBucketMigrationProgressForWorkspaces(workspaces: Iterable[WorkspaceName]): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]]
  def getBucketMigrationProgressForWorkspaces(workspaces: Iterable[WorkspaceName]): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]]
  def adminGetBucketMigrationProgressForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]]
  def getBucketMigrationProgressForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, Option[MultiregionalBucketMigrationProgress]]]
  def adminGetBucketMigrationAttemptsForWorkspace(workspaceName: WorkspaceName): Future[List[MultiregionalBucketMigrationMetadata]]
  def getBucketMigrationAttemptsForWorkspace(workspaceName: WorkspaceName): Future[List[MultiregionalBucketMigrationMetadata]]
  def adminGetBucketMigrationAttemptsForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]]
  def getBucketMigrationAttemptsForBillingProject(billingProjectName: RawlsBillingProjectName): Future[Map[String, List[MultiregionalBucketMigrationMetadata]]]
  def adminMigrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata]
  def migrateWorkspaceBucket(workspaceName: WorkspaceName): Future[MultiregionalBucketMigrationMetadata]
  def adminMigrateAllWorkspaceBuckets(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[MultiregionalBucketMigrationMetadata]]
  def migrateAllWorkspaceBuckets(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[MultiregionalBucketMigrationMetadata]]
  def adminMigrateWorkspaceBucketsInBillingProject(billingProjectName: RawlsBillingProjectName): Future[Iterable[MultiregionalBucketMigrationMetadata]]
  def migrateWorkspaceBucketsInBillingProject(billingProjectName: RawlsBillingProjectName): Future[Iterable[MultiregionalBucketMigrationMetadata]]
}

