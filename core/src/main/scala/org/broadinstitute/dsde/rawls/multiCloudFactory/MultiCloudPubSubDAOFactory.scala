package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.google.{DisabledHttpGooglePubSubDAO, GooglePubSubDAO, HttpGooglePubSubDAO}

import scala.concurrent.ExecutionContext

object MultiCloudPubSubDAOFactory {
  def createPubSubDAO(workbenchMetricBaseName: String,
                      serviceProject: String,
                      config: Config,
                      cloudProvider: String
                                   )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpGooglePubSubDAO(
          config,
          serviceProject: String,
          workbenchMetricBaseName
        )
      case "azure" =>
        new DisabledHttpGooglePubSubDAO(
          config,
          serviceProject: String,
          workbenchMetricBaseName
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
