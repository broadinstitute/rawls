package org.broadinstitute.dsde.rawls.multiCloudFactory
import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdmin
import org.typelevel.log4cats.StructuredLogger

object MultiCloudGoogleTopicAdminFactory {
  def createMultiCloudGoogleTopicAdmin[F[_]: Async](
    appConfigManager: MultiCloudAppConfigManager
  )(implicit F: Sync[F] with Temporal[F], logger: StructuredLogger[F]): Resource[F, GoogleTopicAdmin[F]] =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        GoogleTopicAdmin.fromCredentialPath(pathToCredentialJson)
      case Azure =>
        Resource.pure[F, GoogleTopicAdmin[F]](newDisabledService[GoogleTopicAdmin[F]])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
