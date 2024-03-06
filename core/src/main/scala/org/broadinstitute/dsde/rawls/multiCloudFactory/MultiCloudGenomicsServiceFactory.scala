package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.genomics.{GenomicsService, GenomicsServiceRequest}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudGenomicsServiceFactory {
  def createMultiCloudGenomicsService(appConfigManager: MultiCloudAppConfigManager,
                                      dataSource: SlickDataSource,
                                      gcsDAO: GoogleServicesDAO
  )(implicit executionContext: ExecutionContext): RawlsRequestContext => GenomicsServiceRequest =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        GenomicsService.constructor(dataSource, gcsDAO)
      case "azure" =>
        _ => newDisabledService[GenomicsServiceRequest]
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
