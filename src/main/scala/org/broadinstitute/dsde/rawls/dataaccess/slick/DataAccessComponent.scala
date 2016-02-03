package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.driver.JdbcProfile

class DataAccessComponent(val driver: JdbcProfile)
  extends DriverComponent
  with PendingBucketDeletionComponent {

  import driver.api._

  lazy val schema =
    pendingBucketDeletions.schema
}
