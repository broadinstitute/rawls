package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{HttpImportServiceDAO, ImportServiceDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object ImportServiceDAOFactory {
  def createImportServiceDAO(
    appConfigManager: RawlsConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): ImportServiceDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new HttpImportServiceDAO(appConfigManager.conf.getString("avroUpsertMonitor.cwds"))
      case Azure =>
        newDisabledService[ImportServiceDAO]
    }
}
