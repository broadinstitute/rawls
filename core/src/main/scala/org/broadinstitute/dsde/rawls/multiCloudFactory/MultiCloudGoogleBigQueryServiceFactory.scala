package org.broadinstitute.dsde.rawls.multiCloudFactory

import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryFactoryService, GoogleBigQueryServiceFactory}
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object MultiCloudGoogleBigQueryServiceFactory {
  def createMultiGoogleBigQueryServiceFactory(
    appConfigManager: MultiCloudAppConfigManager
  )(implicit executionContext: ExecutionContext): GoogleBigQueryFactoryService =
    appConfigManager.cloudProvider match {
      case Gcp =>
        val pathToCredentialJson = appConfigManager.gcsConfig.getString("pathToCredentialJson")
        new GoogleBigQueryServiceFactory(pathToCredentialJson)(executionContext)
      case Azure =>
        new GoogleBigQueryFactoryService {

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
