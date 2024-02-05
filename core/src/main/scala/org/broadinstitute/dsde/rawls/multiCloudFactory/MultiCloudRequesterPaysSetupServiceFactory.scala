package org.broadinstitute.dsde.rawls.multiCloudFactory

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{BondApiDAO, DisabledRequesterPaysSetupService, GoogleServicesDAO, RequesterPaysSetup, RequesterPaysSetupService, SlickDataSource}

object MultiCloudRequesterPaysSetupServiceFactory {
  def createAccessContextManager(dataSource: SlickDataSource,
                                 googleServicesDAO: GoogleServicesDAO,
                                 bondApiDAO: BondApiDAO,
                                 requesterPaysRole: String,
                                 cloudProvider: String
                                )(implicit executionContext: ExecutionContext): RequesterPaysSetup = {
    cloudProvider match {
      case "gcp" =>
        new RequesterPaysSetupService(dataSource, googleServicesDAO, bondApiDAO, requesterPaysRole)
      case "azure" =>
        new DisabledRequesterPaysSetupService
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
