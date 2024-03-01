package org.broadinstitute.dsde.rawls.multiCloudFactory
import cats.effect.{Async, Resource, Sync, Temporal}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledGoogleTopicAdmin
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdmin
import org.typelevel.log4cats.StructuredLogger

object MultiCloudGoogleTopicAdminFactory {
  def createMultiCloudGoogleTopicAdmin[F[_]: Async](appConfigManager: MultiCloudAppConfigManager)(implicit F: Sync[F] with Temporal[F],
                                                                                                  logger: StructuredLogger[F]
  ): Resource[F, GoogleTopicAdmin[F]] =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        GoogleTopicAdmin.fromCredentialPath(pathToCredentialJson)
      case "azure" =>
        Resource.pure[F, GoogleTopicAdmin[F]](new DisabledGoogleTopicAdmin[F])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}

