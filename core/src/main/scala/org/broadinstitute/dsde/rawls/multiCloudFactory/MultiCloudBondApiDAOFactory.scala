package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.disabled.DisabledHttpBondApiDAO
import org.broadinstitute.dsde.rawls.dataaccess.{BondApiDAO, HttpBondApiDAO}

import scala.concurrent.ExecutionContext

object MultiCloudBondApiDAOFactory {
  def createMultiCloudBondApiDAO(appConfigManager: MultiCloudAppConfigManager
                                )(implicit system: ActorSystem, executionContext: ExecutionContext): BondApiDAO = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val bondConfig = appConfigManager.conf.getConfig("bond")
        new HttpBondApiDAO(bondConfig.getString("baseUrl"))
      case "azure" =>
        new DisabledHttpBondApiDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
