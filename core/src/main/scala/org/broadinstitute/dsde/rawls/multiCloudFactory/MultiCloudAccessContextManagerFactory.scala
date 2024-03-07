package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudAccessContextManagerFactory {
  def createAccessContextManager(metricsPrefix: String, appConfigManager: MultiCloudAppConfigManager)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): AccessContextManagerDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val clientEmail = gcsConfig.getString("serviceClientEmail")
        val serviceProject = gcsConfig.getString("serviceProject")
        val appName = gcsConfig.getString("appName")
        val pemFile = gcsConfig.getString("pathToPem")
        new HttpGoogleAccessContextManagerDAO(
          clientEmail,
          pemFile,
          appName,
          serviceProject,
          metricsPrefix
        )
      case None =>
        newDisabledService[AccessContextManagerDAO]
    }
}
