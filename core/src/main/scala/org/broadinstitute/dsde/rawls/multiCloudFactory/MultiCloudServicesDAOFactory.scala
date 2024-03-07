package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import cats.effect.IO
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.gson.GsonFactory
import org.broadinstitute.dsde.rawls.AppDependencies
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.rawls.util.ScalaConfig.EnhancedScalaConfig

import java.io.StringReader
import scala.concurrent.ExecutionContext

object MultiCloudServicesDAOFactory {
  def createHttpMultiCloudServicesDAO(appConfigManager: MultiCloudAppConfigManager,
                                      appDependencies: AppDependencies[IO],
                                      metricsPrefix: String,
                                      accessContextManagerDAO: AccessContextManagerDAO
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GoogleServicesDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val gcsConfig = appConfigManager.gcsConfig
        val jsonFactory = GsonFactory.getDefaultInstance
        val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("secrets")))
        new HttpGoogleServicesDAO(
          clientSecrets,
          gcsConfig.getString("serviceClientEmail"),
          gcsConfig.getString("subEmail"),
          gcsConfig.getString("pathToPem"),
          gcsConfig.getString("appsDomain"),
          gcsConfig.getString("groupsPrefix"),
          gcsConfig.getString("appName"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getString("billingPemEmail"),
          gcsConfig.getString("pathToBillingPem"),
          gcsConfig.getString("billingEmail"),
          gcsConfig.getString("billingGroupEmail"),
          googleStorageService = appDependencies.googleStorageService,
          workbenchMetricBaseName = metricsPrefix,
          proxyNamePrefix = gcsConfig.getStringOr("proxyNamePrefix", ""),
          terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
          terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole"),
          accessContextManagerDAO = accessContextManagerDAO,
          resourceBufferJsonFile = gcsConfig.getString("pathToResourceBufferJson")
        )
      case Azure =>
        newDisabledService[GoogleServicesDAO]
    }
}
