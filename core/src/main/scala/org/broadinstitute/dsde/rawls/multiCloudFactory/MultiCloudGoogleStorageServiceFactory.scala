package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.Uri
import org.typelevel.log4cats.StructuredLogger

object MultiCloudGoogleStorageServiceFactory {
  def createMultiCloudGoogleServiceHttp[F[_]: Async](
    appConfigManager: MultiCloudAppConfigManager
  )(implicit F: Sync[F] with Temporal[F], logger: StructuredLogger[F]): Resource[F, GoogleStorageService[F]] =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val googleApiUri = Uri.unsafeFromString(appConfigManager.gcsConfig.getString("google-api-uri"))
        // val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)
        // BlazeClientBuilder(executionContext).resource.flatMap { httpClient =>
        GoogleStorageService.resource[F](pathToCredentialJson,
                                         None,
                                         Option(GoogleProject(appConfigManager.gcsConfig.getString("serviceProject")))
        )
      // }
      case "azure" =>
        Resource.pure[F, GoogleStorageService[F]](newDisabledService[GoogleStorageService[F]])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
