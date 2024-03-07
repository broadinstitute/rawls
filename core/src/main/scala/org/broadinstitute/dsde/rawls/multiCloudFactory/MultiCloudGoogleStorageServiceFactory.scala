package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.typelevel.log4cats.StructuredLogger

object MultiCloudGoogleStorageServiceFactory {
  def createMultiCloudGoogleServiceHttp[F[_]: Async](
    appConfigManager: MultiCloudAppConfigManager
  )(implicit F: Sync[F] with Temporal[F], logger: StructuredLogger[F]): Resource[F, GoogleStorageService[F]] =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        GoogleStorageService
          .resource[F](pathToCredentialJson, None, Option(GoogleProject(gcsConfig.getString("serviceProject"))))
      case None =>
        Resource.pure[F, GoogleStorageService[F]](newDisabledService[GoogleStorageService[F]])
    }
}
