package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.{DisabledGoogleServiceHttp, DisabledGoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.google2.{GoogleServiceHttp, GoogleStorageTransferService, NotificationCreaterConfig}
import org.http4s.Uri
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import org.http4s.blaze.client.BlazeClientBuilder

object MultiCloudGoogleServiceHttpFactory {
  def createMultiCloudGoogleServiceHttp[F[_]: Async](appConfigManager: MultiCloudAppConfigManager, executionContext: ExecutionContext)(implicit
                                                                                                        F: Sync[F] with Temporal[F],
                                                                                                        logger: StructuredLogger[F]
  ): Resource[F, GoogleServiceHttp[F]] =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val googleApiUri = Uri.unsafeFromString(appConfigManager.gcsConfig.getString("google-api-uri"))
        val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)
        BlazeClientBuilder(executionContext).resource.flatMap { httpClient =>
          GoogleServiceHttp.withRetryAndLogging(httpClient, metadataNotificationConfig)
        }
      case "azure" =>
        Resource.pure[F, GoogleServiceHttp[F]](new DisabledGoogleServiceHttp[F])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
