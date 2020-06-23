package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.charset.Charset

import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.google.auth.oauth2.ServiceAccountCredentials
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.commons.io.IOUtils

import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService

import scala.concurrent.ExecutionContext

class GoogleBigQueryServiceFactory(blocker: Blocker)(implicit executionContext: ExecutionContext) {

  implicit lazy val logger: _root_.io.chrisdavenport.log4cats.StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit lazy val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(executionContext)
  implicit lazy val timer: Timer[IO] = cats.effect.IO.timer(executionContext)

  def getServiceForPet(petKey: String): cats.effect.Resource[IO, GoogleBigQueryService[IO]] = {
    val petCredentials = ServiceAccountCredentials.fromStream(IOUtils.toInputStream(petKey, Charset.defaultCharset))
    GoogleBigQueryService.resource[IO](petCredentials, blocker)
  }

}
