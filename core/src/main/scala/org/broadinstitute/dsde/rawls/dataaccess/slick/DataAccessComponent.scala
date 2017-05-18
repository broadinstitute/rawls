package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.driver.JdbcDriver

import scala.concurrent.ExecutionContext

class DataAccessComponent(val driver: JdbcDriver, val batchSize: Int)(implicit val executionContext: ExecutionContext)
extends DriverComponent with DataAccess