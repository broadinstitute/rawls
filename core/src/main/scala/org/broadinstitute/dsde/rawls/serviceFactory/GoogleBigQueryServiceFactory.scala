package org.broadinstitute.dsde.rawls.serviceFactory

import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, GoogleBigQueryServiceFactoryImpl}
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object GoogleBigQueryServiceFactory {
  def createGoogleBigQueryServiceFactory(
    appConfigManager: RawlsConfigManager
  )(implicit executionContext: ExecutionContext): GoogleBigQueryServiceFactory =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
        new GoogleBigQueryServiceFactoryImpl(pathToCredentialJson)(executionContext)
      case None =>
        new GoogleBigQueryServiceFactory {

          override def getServiceForPet(petKey: String,
                                        projectId: GoogleProject
          ): Resource[IO, GoogleBigQueryService[IO]] =
            Resource.pure[IO, GoogleBigQueryService[IO]](newDisabledService[GoogleBigQueryService[IO]])

          override def getServiceForProject(projectId: GoogleProjectId): Resource[IO, GoogleBigQueryService[IO]] =
            Resource.pure[IO, GoogleBigQueryService[IO]](newDisabledService[GoogleBigQueryService[IO]])

          override def getServiceFromJson(json: String,
                                          projectId: GoogleProject
          ): Resource[IO, GoogleBigQueryService[IO]] =
            Resource.pure[IO, GoogleBigQueryService[IO]](newDisabledService[GoogleBigQueryService[IO]])
        }
    }
}
