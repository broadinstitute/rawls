package org.broadinstitute.dsde.test.api

import cats.effect.IO
import org.typelevel.log4cats.slf4j.Slf4jLogger


//noinspection TypeAnnotation
trait RawlsTestSuite {
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
}
