package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.{IO, Temporal}
import com.google.api.client.util.Charsets
import com.google.auth.oauth2.ServiceAccountCredentials
import org.apache.commons.io.IOUtils
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import scala.concurrent.ExecutionContext

/**
 * DataRepoEntityProvider, and potential future callers of this class, need to create a new
 * GoogleBigQueryService for every request. They do this because they need to set different
 * user credentials for each request.
 *
 * This factory class contains boilerplate and allows callers to easily and quickly get
 * a new service instance for each user's credentials.
 */
class GoogleBigQueryServiceFactory(pathToCredentialJson: String)(implicit executionContext: ExecutionContext)
  extends GoogleBigQueryFactoryService{

  implicit lazy val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit lazy val timer: Temporal[IO] = Temporal[IO]

  def getServiceForPet(petKey: String,
                       projectId: GoogleProject
  ): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val petCredentials = ServiceAccountCredentials.fromStream(IOUtils.toInputStream(petKey, Charset.defaultCharset))
    GoogleBigQueryService.resource[IO](petCredentials, projectId)
  }

  def getServiceForProject(projectId: GoogleProjectId): cats.effect.Resource[IO, GoogleBigQueryService[IO]] =
    GoogleBigQueryService.resource[IO](pathToCredentialJson, GoogleProject(projectId.value))

  def getServiceFromJson(json: String,
                         projectId: GoogleProject
  ): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val creds = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(json.getBytes(Charsets.UTF_8)))
    GoogleBigQueryService.resource[IO](creds, projectId)
  }
}
