package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.charset.Charset
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.util.Charsets
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.commons.io.IOUtils
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import java.io.ByteArrayInputStream
import scala.concurrent.ExecutionContext

/**
 * DataRepoEntityProvider, and potential future callers of this class, need to create a new
 * GoogleBigQueryService for every request. They do this because they need to set different
 * user credentials for each request.
 *
 * This factory class contains boilerplate and allows callers to easily and quickly get
 * a new service instance for each user's credentials.
 */
class GoogleBigQueryServiceFactory(pathToCredentialJson: String, blocker: Blocker)(implicit executionContext: ExecutionContext) {

  implicit lazy val logger = Slf4jLogger.getLogger[IO]
  implicit lazy val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(executionContext)
  implicit lazy val timer: Timer[IO] = cats.effect.IO.timer(executionContext)

  def getServiceForPet(petKey: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val petCredentials = ServiceAccountCredentials.fromStream(IOUtils.toInputStream(petKey, Charset.defaultCharset))
    GoogleBigQueryService.resource[IO](petCredentials, blocker, projectId)
  }

  def getServiceForProject(projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    GoogleBigQueryService.resource[IO](pathToCredentialJson, projectId, blocker)
  }

  def getServiceForProject(projectId: GoogleProjectId): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    getServiceForProject(GoogleProject(projectId.value))
  }

  def getServiceFromJson(json: String, projectId: GoogleProject): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val creds = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(json.getBytes(Charsets.UTF_8)))
    GoogleBigQueryService.resource[IO](creds, blocker, projectId)
  }

}
