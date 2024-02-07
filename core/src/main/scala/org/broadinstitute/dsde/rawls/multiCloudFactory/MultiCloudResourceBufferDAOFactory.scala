package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.dataaccess.DisabledHttpResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.{HttpResourceBufferDAO, ResourceBufferDAO}

import scala.concurrent.ExecutionContext

object MultiCloudResourceBufferDAOFactory {
  def createResourceBuffer(config: Config,
                           clientServiceAccountCreds: Credential,
                           cloudProvider: String)(implicit system: ActorSystem, executionContext: ExecutionContext): ResourceBufferDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpResourceBufferDAO(ResourceBufferConfig(config.getConfig("resourceBuffer")), clientServiceAccountCreds)
      case "azure" =>
        new DisabledHttpResourceBufferDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
