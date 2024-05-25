package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object PubSubDAOFactory {
  def createPubSubDAO(appConfigManager: RawlsConfigManager, metricsPrefix: String)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): GooglePubSubDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        createPubSubDAOForProject(metricsPrefix, gcsConfig, gcsConfig.getString("serviceProject"))
      case None =>
        newDisabledService[GooglePubSubDAO]
    }

  private def createPubSubDAOForProject(metricsPrefix: String, gcsConfig: Config, project: String)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): GooglePubSubDAO = {
    val clientEmail = gcsConfig.getString("serviceClientEmail")
    val appName = gcsConfig.getString("appName")
    val pathToPem = gcsConfig.getString("pathToPem")
    new HttpGooglePubSubDAO(
      clientEmail,
      pathToPem,
      appName,
      project,
      workbenchMetricBaseName = metricsPrefix
    )
  }
}
