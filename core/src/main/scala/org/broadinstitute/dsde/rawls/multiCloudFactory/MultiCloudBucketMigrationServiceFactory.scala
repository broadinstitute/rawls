package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigration, BucketMigrationService}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudBucketMigrationServiceFactory {
  def createMultiCloudBucketMigrationService(appConfigManager: MultiCloudAppConfigManager,
                                             slickDataSource: SlickDataSource,
                                             samDAO: SamDAO,
                                             gcsDAO: GoogleServicesDAO
  )(implicit executionContext: ExecutionContext): RawlsRequestContext => BucketMigration =
    appConfigManager.cloudProvider match {
      case Gcp =>
        BucketMigrationService.constructor(slickDataSource, samDAO, gcsDAO)
      case Azure =>
        _ => newDisabledService[BucketMigration]
    }
}
