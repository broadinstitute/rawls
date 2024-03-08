package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object AccessContextManagerFactory {
  def createAccessContextManager(metricsPrefix: String, appConfigManager: RawlsConfigManager)(implicit
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
