package org.broadinstitute.dsde.rawls.serviceFactory
import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdmin
import org.typelevel.log4cats.StructuredLogger

object GoogleTopicAdminFactory {
  def createGoogleTopicAdmin[F[_]: Async](
    appConfigManager: MultiCloudAppConfigManager
  )(implicit F: Sync[F] with Temporal[F], logger: StructuredLogger[F]): Resource[F, GoogleTopicAdmin[F]] =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        GoogleTopicAdmin.fromCredentialPath(pathToCredentialJson)
      case None =>
        Resource.pure[F, GoogleTopicAdmin[F]](newDisabledService[GoogleTopicAdmin[F]])
    }
}
