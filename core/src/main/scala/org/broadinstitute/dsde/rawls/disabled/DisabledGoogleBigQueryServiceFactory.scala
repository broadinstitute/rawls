package org.broadinstitute.dsde.rawls.disabled

import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess.GoogleBigQueryFactoryService
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import cats.effect.Resource

class DisabledGoogleBigQueryServiceFactory extends GoogleBigQueryFactoryService{
  def getServiceForPet(petKey: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]] =
    throw new NotImplementedError("getServiceForPet is not implemented for Azure.")
  def getServiceForProject(projectId: GoogleProjectId): cats.effect.Resource[IO, GoogleBigQueryService[IO]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")
  def getServiceFromJson(json: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]] =
    Resource.pure[IO, GoogleBigQueryService[IO]](new DisabledGoogleBigQueryService[IO])

}
