package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{HttpImportServiceDAO, ImportServiceDAO}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudImportServiceDAOFactory {
  def createMultiCloudImportServiceDAO(
    appConfigManager: MultiCloudAppConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): ImportServiceDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new HttpImportServiceDAO(appConfigManager.conf.getString("avroUpsertMonitor.server"))
      case "azure" =>
        newDisabledService[ImportServiceDAO]
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
