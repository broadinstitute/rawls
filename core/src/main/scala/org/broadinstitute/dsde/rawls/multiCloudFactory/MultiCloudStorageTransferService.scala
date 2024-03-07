package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.typelevel.log4cats.StructuredLogger

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object MultiCloudStorageTransferService {
  def createMultiCloudStorageTransferService[F[_]: Async](appConfigManager: MultiCloudAppConfigManager)(implicit
    F: Sync[F] with Temporal[F],
    logger: StructuredLogger[F]
  ): Resource[F, GoogleStorageTransferService[F]] =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
        val jsonCreds =
          try jsonFileSource.mkString
          finally jsonFileSource.close()
        val saCredentials = Async[F].delay(
          ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(jsonCreds.getBytes(StandardCharsets.UTF_8))
          )
        )
        Resource.eval(saCredentials).flatMap { creds =>
          GoogleStorageTransferService.resource(creds)
        }
      case Azure =>
        Resource.pure[F, GoogleStorageTransferService[F]](newDisabledService[GoogleStorageTransferService[F]])
      case _ => Resource.eval(Async[F].raiseError(new IllegalArgumentException("Invalid cloud provider")))
    }
}
