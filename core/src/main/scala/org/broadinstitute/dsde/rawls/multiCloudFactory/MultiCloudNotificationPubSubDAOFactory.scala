package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledHttpGooglePubSubDAO, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

import scala.concurrent.ExecutionContext

object MultiCloudNotificationPubSubDAOFactory {
  def createMultiCloudNotificationPubSubDAO(workbenchMetricBaseName: String,
                                            config:  Config,
                                            cloudProvider: String
                                   )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO = {
    val clientEmail = config.getString("serviceClientEmail")
    val pathToPem = config.getString("pathToPem")
    val appName = config.getString("appName")
    val serviceProject = config.getString("serviceProject")
    cloudProvider match {
      case "gcp" =>
        new org.broadinstitute.dsde.workbench.google.HttpGooglePubSubDAO(
          clientEmail,
          pathToPem,
          appName,
          serviceProject,
          workbenchMetricBaseName
        )
      case "azure" =>
        new DisabledHttpGooglePubSubDAO(
          clientEmail,
          pathToPem,
          appName,
          serviceProject,
          workbenchMetricBaseName
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
