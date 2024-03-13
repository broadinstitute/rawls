package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigrationService, BucketMigrationServiceImpl}
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object BucketMigrationServiceFactory {
  def createBucketMigrationService(appConfigManager: RawlsConfigManager,
                                   slickDataSource: SlickDataSource,
                                   samDAO: SamDAO,
                                   gcsDAO: GoogleServicesDAO
  )(implicit executionContext: ExecutionContext): RawlsRequestContext => BucketMigrationService =
    appConfigManager.cloudProvider match {
      case Gcp =>
        BucketMigrationServiceImpl.constructor(slickDataSource, samDAO, gcsDAO)
      case Azure =>
        _ => newDisabledService[BucketMigrationService]
    }
}
