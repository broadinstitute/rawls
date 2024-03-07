package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudRequesterPaysSetupServiceFactory {
  def createAccessContextManager(appConfigManager: MultiCloudAppConfigManager,
                                 dataSource: SlickDataSource,
                                 googleServicesDAO: GoogleServicesDAO,
                                 bondApiDAO: BondApiDAO,
                                 requesterPaysRole: String
  )(implicit executionContext: ExecutionContext): RequesterPaysSetup =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new RequesterPaysSetupService(dataSource, googleServicesDAO, bondApiDAO, requesterPaysRole)
      case Azure =>
        newDisabledService[RequesterPaysSetup]
    }
}
