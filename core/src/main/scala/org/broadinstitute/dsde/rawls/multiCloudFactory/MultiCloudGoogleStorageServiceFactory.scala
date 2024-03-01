package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.{DisabledGoogleServiceHttp, DisabledGoogleStorageService}
import org.broadinstitute.dsde.workbench.google2.{GoogleServiceHttp, GoogleStorageService, NotificationCreaterConfig}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

object MultiCloudGoogleStorageServiceFactory {
  def createMultiCloudGoogleServiceHttp[F[_]: Async](appConfigManager: MultiCloudAppConfigManager)(implicit F: Sync[F] with Temporal[F],
                                                                                                   logger: StructuredLogger[F]
  ): Resource[F, GoogleStorageService[F]] =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val googleApiUri = Uri.unsafeFromString(appConfigManager.gcsConfig.getString("google-api-uri"))
        //val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)
        //BlazeClientBuilder(executionContext).resource.flatMap { httpClient =>
          GoogleStorageService.resource[F](pathToCredentialJson, None, Option(GoogleProject(appConfigManager.gcsConfig.getString("serviceProject"))))
       // }
      case "azure" =>
        Resource.pure[F, GoogleStorageService[F]](new DisabledGoogleStorageService[F])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
