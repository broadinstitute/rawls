package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.{HttpResourceBufferDAO, ResourceBufferDAO}
import org.broadinstitute.dsde.rawls.disabled.DisabledHttpResourceBufferDAO

import scala.concurrent.ExecutionContext

object MultiCloudResourceBufferDAOFactory {
  def createResourceBuffer(appConfigManager: MultiCloudAppConfigManager, clientServiceAccountCreds: Credential)(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): ResourceBufferDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new HttpResourceBufferDAO(ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer")),
                                  clientServiceAccountCreds
        )
      case "azure" =>
        new DisabledHttpResourceBufferDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
