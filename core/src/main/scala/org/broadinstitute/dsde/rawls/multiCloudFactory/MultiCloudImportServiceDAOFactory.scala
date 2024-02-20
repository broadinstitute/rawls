package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{HttpImportServiceDAO, ImportServiceDAO}
import org.broadinstitute.dsde.rawls.disabled.DisabledImportServiceDAO

import scala.concurrent.ExecutionContext

object MultiCloudImportServiceDAOFactory {
  def createMultiCloudImportServiceDAO(appConfigManager: MultiCloudAppConfigManager
                                      )(implicit system: ActorSystem, executionContext: ExecutionContext): ImportServiceDAO = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new HttpImportServiceDAO(appConfigManager.conf.getString("avroUpsertMonitor.server"))
      case "azure" =>
        new DisabledImportServiceDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
