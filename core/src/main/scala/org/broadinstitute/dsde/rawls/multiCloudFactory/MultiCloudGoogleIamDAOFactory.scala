package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledHttpGoogleIamDAO
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, GoogleIamDAO, HttpGoogleIamDAO}
import scala.concurrent.ExecutionContext

object MultiCloudGoogleIamDAOFactory {
  def createMultiCloudGoogleIamDAO(appConfigManager: MultiCloudAppConfigManager, metricsPrefix: String
                                  )(implicit executionContext: ExecutionContext, system: ActorSystem): GoogleIamDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val jsonCreds =
          try jsonFileSource.mkString
          finally jsonFileSource.close()

        new HttpGoogleIamDAO(appConfigManager.gcsConfig.getString("appName"), GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(
          system,
          executionContext
        )
      case "azure" =>
        new DisabledHttpGoogleIamDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }

}
