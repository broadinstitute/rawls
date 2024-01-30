package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, DisabledHttpGoogleAccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}

import scala.concurrent.ExecutionContext
import com.typesafe.config.Config

object MultiCloudAccessContextManagerFactory {
  def createAccessContextManager(
                 metricsPrefix: String,
                 config: Config,
                 cloudProvider: String
                         )(implicit system: ActorSystem, executionContext: ExecutionContext): AccessContextManagerDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpGoogleAccessContextManagerDAO(
          config,
          metricsPrefix
        )
      case "azure" =>
        new DisabledHttpGoogleAccessContextManagerDAO(
          config,
          metricsPrefix
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
