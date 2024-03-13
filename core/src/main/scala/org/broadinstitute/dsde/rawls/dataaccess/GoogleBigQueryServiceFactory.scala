package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.IO
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait GoogleBigQueryServiceFactory {
  def getServiceForPet(petKey: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]]
  def getServiceForProject(projectId: GoogleProjectId): cats.effect.Resource[IO, GoogleBigQueryService[IO]]
  def getServiceFromJson(json: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]]
}
