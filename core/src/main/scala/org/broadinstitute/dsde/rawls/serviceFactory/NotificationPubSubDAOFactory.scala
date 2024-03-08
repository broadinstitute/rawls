package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Pem
import org.broadinstitute.dsde.workbench.google.{GooglePubSubDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.io.File
import scala.concurrent.ExecutionContext

object NotificationPubSubDAOFactory {
  def createNotificationPubSubDAO(appConfigManager: MultiCloudAppConfigManager,
                                  workbenchMetricBaseName: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val clientEmail = gcsConfig.getString("serviceClientEmail")
        val pemFile = gcsConfig.getString("pathToPem")
        val appName = gcsConfig.getString("appName")
        val serviceProject = gcsConfig.getString("serviceProject")
        new HttpGooglePubSubDAO(appName,
                                Pem(WorkbenchEmail(clientEmail), new File(pemFile)),
                                workbenchMetricBaseName,
                                serviceProject
        )
      case None =>
        newDisabledService[GooglePubSubDAO]
    }
}
