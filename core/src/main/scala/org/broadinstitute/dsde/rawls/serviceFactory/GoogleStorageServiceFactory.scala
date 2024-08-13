package org.broadinstitute.dsde.rawls.serviceFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.typelevel.log4cats.StructuredLogger

object GoogleStorageServiceFactory {
  def createGoogleStorageService[F[_]: Async](
    appConfigManager: RawlsConfigManager
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
