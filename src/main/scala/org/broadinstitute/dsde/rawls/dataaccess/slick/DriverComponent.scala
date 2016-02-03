package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.driver.JdbcProfile

trait DriverComponent {
  val driver: JdbcProfile
}
