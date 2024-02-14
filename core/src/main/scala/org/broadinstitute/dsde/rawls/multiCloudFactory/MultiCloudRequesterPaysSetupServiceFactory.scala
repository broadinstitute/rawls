package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.disabled.DisabledRequesterPaysSetupService
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{BondApiDAO, GoogleServicesDAO, RequesterPaysSetup, RequesterPaysSetupService, SlickDataSource}

object MultiCloudRequesterPaysSetupServiceFactory {
  def createAccessContextManager(appConfigManager: MultiCloudAppConfigManager,
                                 dataSource: SlickDataSource,
                                 googleServicesDAO: GoogleServicesDAO,
                                 bondApiDAO: BondApiDAO,
                                 requesterPaysRole: String
                                )(implicit executionContext: ExecutionContext): RequesterPaysSetup = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new RequesterPaysSetupService(dataSource, googleServicesDAO, bondApiDAO, requesterPaysRole)
      case "azure" =>
        new DisabledRequesterPaysSetupService
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
