package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, GoogleIamDAO, HttpGoogleIamDAO}

import scala.concurrent.ExecutionContext

object MultiCloudGoogleIamDAOFactory {
  def createMultiCloudGoogleIamDAO(appConfigManager: MultiCloudAppConfigManager, metricsPrefix: String)(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): GoogleIamDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val jsonCreds =
          try jsonFileSource.mkString
          finally jsonFileSource.close()

        new HttpGoogleIamDAO(gcsConfig.getString("appName"), GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(
          system,
          executionContext
        )
      case None =>
        newDisabledService[GoogleIamDAO]
    }

}
