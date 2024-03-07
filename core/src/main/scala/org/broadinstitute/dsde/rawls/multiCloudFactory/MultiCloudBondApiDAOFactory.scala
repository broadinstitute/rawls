package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{BondApiDAO, HttpBondApiDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudBondApiDAOFactory {
  def createMultiCloudBondApiDAO(
    appConfigManager: MultiCloudAppConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): BondApiDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val bondConfig = appConfigManager.conf.getConfig("bond")
        new HttpBondApiDAO(bondConfig.getString("baseUrl"))
      case Azure =>
        newDisabledService[BondApiDAO]
    }
}
