package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryFactoryService, GoogleBigQueryServiceFactory}
import org.broadinstitute.dsde.rawls.disabled.DisabledGoogleBigQueryServiceFactory

import scala.concurrent.ExecutionContext

object MultiCloudGoogleBigQueryServiceFactory {
  def createMultiGoogleBigQueryServiceFactory(appConfigManager: MultiCloudAppConfigManager
                                             )(implicit executionContext: ExecutionContext): GoogleBigQueryFactoryService =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        new GoogleBigQueryServiceFactory(pathToCredentialJson)(executionContext)
      case "azure" =>
        new DisabledGoogleBigQueryServiceFactory
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
