package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledHttpMethodRepoDAO, HttpMethodRepoDAO, MethodRepoDAO}
import org.broadinstitute.dsde.rawls.model.{Agora, Dockstore}

import scala.concurrent.ExecutionContext

object MultiCloudMethodRepoDAOFactory {
  def createMultiCloudMethodRepoDAO(metricsPrefix: String,
                                    config: Config,
                                    cloudProvider: String
                                   )(implicit system: ActorSystem, executionContext: ExecutionContext): MethodRepoDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpMethodRepoDAO(
          MethodRepoConfig.apply[Agora.type](config.getConfig("agora")),
          MethodRepoConfig.apply[Dockstore.type](config.getConfig("dockstore")),
          metricsPrefix
        )
      case "azure" =>
        new DisabledHttpMethodRepoDAO(
          MethodRepoConfig.apply[Agora.type](config.getConfig("agora")),
          MethodRepoConfig.apply[Dockstore.type](config.getConfig("dockstore")),
          metricsPrefix
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
