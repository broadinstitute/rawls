package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.DisabledGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{GoogleCredentialMode, Json}
import org.broadinstitute.dsde.workbench.google.{AbstractHttpGoogleDAO, GoogleBigQueryDAO, HttpGoogleBigQueryDAO}

import scala.concurrent.ExecutionContext

object MultiCloudBigQueryDAOFactory {
  def createHttpMultiCloudBigQueryDAO(config: Config,
                                      googleCredentialMode: GoogleCredentialMode,
                                      metricsPrefix: String,
                                      cloudProvider: String
                                     )(implicit system: ActorSystem, executionContext: ExecutionContext): GoogleBigQueryDAO = {

    cloudProvider match {
      case "gcp" =>
        new HttpGoogleBigQueryDAO(
          config.getString("appName"),
          googleCredentialMode,
          workbenchMetricBaseName = metricsPrefix
        )
      case "azure" =>
        new DisabledGoogleBigQueryDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
