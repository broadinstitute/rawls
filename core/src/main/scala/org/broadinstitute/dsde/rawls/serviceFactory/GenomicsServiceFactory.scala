package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.genomics.{GenomicsService, GenomicsServiceImpl}
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object GenomicsServiceFactory {
  def createGenomicsService(appConfigManager: RawlsConfigManager,
                            dataSource: SlickDataSource,
                            gcsDAO: GoogleServicesDAO
  )(implicit executionContext: ExecutionContext): RawlsRequestContext => GenomicsService =
    appConfigManager.cloudProvider match {
      case Gcp =>
        GenomicsServiceImpl.constructor(dataSource, gcsDAO)
      case Azure =>
        _ => newDisabledService[GenomicsService]
    }
}
