package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.{HttpResourceBufferDAO, ResourceBufferDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudResourceBufferDAOFactory {
  def createResourceBuffer(appConfigManager: MultiCloudAppConfigManager, gcsDAO: GoogleServicesDAO)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): ResourceBufferDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new HttpResourceBufferDAO(ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer")),
                                  gcsDAO.getResourceBufferServiceAccountCredential
        )
      case Azure =>
        newDisabledService[ResourceBufferDAO]
    }
}
