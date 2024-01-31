package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DisabledDrsHubResolver, DisabledMarthaResolver, DrsHubResolver, DrsResolver, MarthaResolver}

import scala.concurrent.ExecutionContext

object MultiCloudDrsResolverFactory {
  def createMultiCloudDrsResolver(config: Config,
                                  cloudProvider: String
                                   )(implicit system: ActorSystem, executionContext: ExecutionContext): DrsResolver = {
    cloudProvider match {
      case "gcp" =>
        if (config.hasPath("drs")) {
          val drsResolverName = config.getString("drs.resolver")
          drsResolverName match {
            case "martha" =>
              val marthaBaseUrl: String = config.getString("drs.martha.baseUrl")
              val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
              new MarthaResolver(marthaUrl)
            case "drshub" =>
              val drsHubBaseUrl: String = config.getString("drs.drshub.baseUrl")
              val drsHubUrl: String = s"$drsHubBaseUrl/api/v4/drs/resolve"
              new DrsHubResolver(drsHubUrl)
          }
        } else {
          val marthaBaseUrl: String = config.getString("martha.baseUrl")
          val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
          new MarthaResolver(marthaUrl)
        }
      case "azure" =>
          new DisabledMarthaResolver
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
