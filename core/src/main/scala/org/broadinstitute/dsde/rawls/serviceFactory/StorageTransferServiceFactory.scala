package org.broadinstitute.dsde.rawls.serviceFactory

import cats.effect.{Async, Resource, Sync, Temporal}
import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.typelevel.log4cats.StructuredLogger

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object StorageTransferServiceFactory {
  def createStorageTransferService[F[_]: Async](appConfigManager: RawlsConfigManager)(implicit
    F: Sync[F] with Temporal[F],
    logger: StructuredLogger[F]
  ): Resource[F, GoogleStorageTransferService[F]] =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
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
      case None =>
        Resource.pure[F, GoogleStorageTransferService[F]](newDisabledService[GoogleStorageTransferService[F]])
    }
}
