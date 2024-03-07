package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
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
    appConfigManager.cloudProvider match {
      case Gcp =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val googleApiUri = Uri.unsafeFromString(appConfigManager.gcsConfig.getString("google-api-uri"))
        val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)
        BlazeClientBuilder(executionContext).resource.flatMap { httpClient =>
          GoogleServiceHttp.withRetryAndLogging(httpClient, metadataNotificationConfig)
        }
      case Azure =>
        Resource.pure[F, GoogleServiceHttp[F]](newDisabledService[GoogleServiceHttp[F]])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
