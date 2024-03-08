package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, GoogleStorageDAO, HttpGoogleStorageDAO}

import scala.concurrent.ExecutionContext

object HttpGoogleStorageDAOFactory {
  def createHttpGoogleStorageDAO(appConfigManager: RawlsConfigManager, metricsPrefix: String)(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): GoogleStorageDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val jsonCreds =
          try jsonFileSource.mkString
          finally jsonFileSource.close()
        new HttpGoogleStorageDAO(gcsConfig.getString("appName"), GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(
          system,
          executionContext
        )
      case None =>
        newDisabledService[GoogleStorageDAO]
    }
}
