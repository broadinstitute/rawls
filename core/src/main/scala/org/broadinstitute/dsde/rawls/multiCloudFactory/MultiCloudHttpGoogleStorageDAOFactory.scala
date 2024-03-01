package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledHttpGoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, GoogleStorageDAO, HttpGoogleStorageDAO}

import scala.concurrent.ExecutionContext

object MultiCloudHttpGoogleStorageDAOFactory {
  def createHttpGoogleStorageDAO(appConfigManager: MultiCloudAppConfigManager, metricsPrefix: String
                                )(implicit executionContext: ExecutionContext, system: ActorSystem): GoogleStorageDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val jsonCreds =
          try jsonFileSource.mkString
          finally jsonFileSource.close()
        new HttpGoogleStorageDAO(appConfigManager.gcsConfig.getString("appName"), GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(system, executionContext)
      case "azure" =>
        new DisabledHttpGoogleStorageDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
