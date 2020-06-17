package org.broadinstitute.dsde.rawls.dataaccess

import java.nio.charset.Charset

import cats.effect.{Blocker, ContextShift, Sync, Timer}
import com.google.auth.oauth2.ServiceAccountCredentials
import io.chrisdavenport.log4cats.StructuredLogger
import org.apache.commons.io.IOUtils
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService

import scala.language.higherKinds

class GoogleBigQueryServiceFactory[F[_]: Sync: ContextShift: Timer: StructuredLogger](blocker: Blocker) {

  def getServiceForPet(petKey: String): cats.effect.Resource[F, GoogleBigQueryService[F]] = {
    val petCredentials = ServiceAccountCredentials.fromStream(IOUtils.toInputStream(petKey, Charset.defaultCharset))
    GoogleBigQueryService.resource[F](petCredentials, blocker)
  }

}
