package org.broadinstitute.dsde.test.api

import cats.effect.IO
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.global

//noinspection TypeAnnotation
trait RawlsTestSuite {
  implicit val cs = IO.contextShift(global)
  implicit val timer = IO.timer(global)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
}
