package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigration, BucketMigrationService, DisabledBucketMigrationService}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext

import scala.concurrent.ExecutionContext

object MultiCloudBucketMigrationServiceFactory {
  def createMultiCloudBucketMigrationService(appConfigManager: MultiCloudAppConfigManager,
                                             slickDataSource: SlickDataSource,
                                             samDAO: SamDAO,
                                             gcsDAO: GoogleServicesDAO
                                             )(implicit executionContext: ExecutionContext): RawlsRequestContext => BucketMigration  = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        BucketMigrationService.constructor(slickDataSource, samDAO, gcsDAO)
      case "azure" =>
        DisabledBucketMigrationService.constructor()
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
