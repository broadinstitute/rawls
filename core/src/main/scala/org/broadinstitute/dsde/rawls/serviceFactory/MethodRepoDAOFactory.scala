package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.{MethodRepoConfig, MultiCloudAppConfigManager}
import org.broadinstitute.dsde.rawls.dataaccess.{HttpMethodRepoDAO, MethodRepoDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.model.{Agora, Dockstore}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MethodRepoDAOFactory {
  def createMethodRepoDAO(appConfigManager: MultiCloudAppConfigManager, metricsPrefix: String)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): MethodRepoDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new HttpMethodRepoDAO(
          MethodRepoConfig.apply[Agora.type](appConfigManager.conf.getConfig("agora")),
          MethodRepoConfig.apply[Dockstore.type](appConfigManager.conf.getConfig("dockstore")),
          metricsPrefix
        )
      case Azure =>
        newDisabledService[MethodRepoDAO]
    }
}
