package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{BondApiDAO, DisabledHttpBondApiDAO, HttpBondApiDAO}

import scala.concurrent.ExecutionContext

object MultiCloudBondApiDAOFactory {
  def createMultiCloudBondApiDAO(config: Config,
                                 cloudProvider: String
                                )(implicit system: ActorSystem, executionContext: ExecutionContext): BondApiDAO = {
    cloudProvider match {
      case "gcp" =>
        val bondConfig = config.getConfig("bond")
        new HttpBondApiDAO(bondConfig.getString("baseUrl"))
      case "azure" =>
        new DisabledHttpBondApiDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
