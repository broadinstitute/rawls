package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.driver.JdbcProfile

import scala.concurrent.ExecutionContext

trait DriverComponent {
  val driver: JdbcProfile
  implicit val executionContext: ExecutionContext
}
