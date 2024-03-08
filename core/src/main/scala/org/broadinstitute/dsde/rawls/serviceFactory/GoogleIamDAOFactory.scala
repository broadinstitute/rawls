package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, GoogleIamDAO, HttpGoogleIamDAO}

import scala.concurrent.ExecutionContext

object GoogleIamDAOFactory {
  def createGoogleIamDAO(appConfigManager: RawlsConfigManager, metricsPrefix: String)(implicit
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
