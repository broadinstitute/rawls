package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.{GoogleServiceHttp, NotificationCreaterConfig}
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

object MultiCloudGoogleServiceHttpFactory {
  def createMultiCloudGoogleServiceHttp[F[_]: Async](appConfigManager: MultiCloudAppConfigManager,
                                                     executionContext: ExecutionContext
  )(implicit
    F: Sync[F] with Temporal[F],
    logger: StructuredLogger[F]
  ): Resource[F, GoogleServiceHttp[F]] =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        val googleApiUri = Uri.unsafeFromString(gcsConfig.getString("google-api-uri"))
        val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)
        BlazeClientBuilder[F].withExecutionContext(executionContext).resource.flatMap { httpClient =>
          GoogleServiceHttp.withRetryAndLogging(httpClient, metadataNotificationConfig)
        }
      case None =>
        Resource.pure[F, GoogleServiceHttp[F]](newDisabledService[GoogleServiceHttp[F]])
    }
}
