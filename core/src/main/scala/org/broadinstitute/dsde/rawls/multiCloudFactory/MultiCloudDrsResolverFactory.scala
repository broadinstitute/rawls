package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DrsHubResolver, DrsResolver, MarthaResolver}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudDrsResolverFactory {
  def createMultiCloudDrsResolver(
    appConfigManager: MultiCloudAppConfigManager
  )(implicit system: ActorSystem, executionContext: ExecutionContext): DrsResolver =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        if (appConfigManager.conf.hasPath("drs")) {
          val drsResolverName = appConfigManager.conf.getString("drs.resolver")
          drsResolverName match {
            case "martha" =>
              val marthaBaseUrl: String = appConfigManager.conf.getString("drs.martha.baseUrl")
              val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
              new MarthaResolver(marthaUrl)
            case "drshub" =>
              val drsHubBaseUrl: String = appConfigManager.conf.getString("drs.drshub.baseUrl")
              val drsHubUrl: String = s"$drsHubBaseUrl/api/v4/drs/resolve"
              new DrsHubResolver(drsHubUrl)
          }
        } else {
          val marthaBaseUrl: String = appConfigManager.conf.getString("martha.baseUrl")
          val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
          new MarthaResolver(marthaUrl)
        }
      case "azure" =>
        newDisabledService[DrsResolver]
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
