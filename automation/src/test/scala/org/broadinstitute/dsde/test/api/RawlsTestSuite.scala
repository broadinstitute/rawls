package org.broadinstitute.dsde.test.api

import cats.effect.IO
import io.chrisdavenport.linebacker.Linebacker
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.global

trait RawlsTestSuite {
  implicit val cs = IO.contextShift(global)
  implicit val timer = IO.timer(global)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  implicit val lineBacker = Linebacker.fromExecutionContext[IO](global)
}
