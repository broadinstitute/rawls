package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{CwdsDAO, HttpCwdsDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object CwdsDAOFactory {
  def createCwdsDAO(
    appConfigManager: RawlsConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): CwdsDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new HttpCwdsDAO(appConfigManager.conf.getString("avroUpsertMonitor.cwds"))
      case Azure =>
        newDisabledService[CwdsDAO]
    }
}
