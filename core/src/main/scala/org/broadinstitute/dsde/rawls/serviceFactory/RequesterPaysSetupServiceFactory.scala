package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object RequesterPaysSetupServiceFactory {
  def createRequesterPaysSetup(appConfigManager: RawlsConfigManager,
                               dataSource: SlickDataSource,
                               googleServicesDAO: GoogleServicesDAO,
                               bondApiDAO: BondApiDAO,
                               requesterPaysRole: String
  )(implicit executionContext: ExecutionContext): RequesterPaysSetupService =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new RequesterPaysSetupServiceImpl(dataSource, googleServicesDAO, bondApiDAO, requesterPaysRole)
      case Azure =>
        newDisabledService[RequesterPaysSetupService]
    }
}
