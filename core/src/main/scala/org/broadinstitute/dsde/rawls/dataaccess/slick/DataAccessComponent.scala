package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.jndi.DirectoryConfig
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

class DataAccessComponent(val driver: JdbcProfile, val batchSize: Int, override val directoryConfig: DirectoryConfig)(implicit val executionContext: ExecutionContext)
extends DriverComponent with DataAccess
