package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

class DataAccessComponent(val driver: JdbcProfile, val batchSize: Int, val fetchSize: Int)(implicit
  val executionContext: ExecutionContext
) extends DriverComponent
    with DataAccess
