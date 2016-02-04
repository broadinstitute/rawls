package org.broadinstitute.dsde.rawls.dataaccess.slick

trait AllComponents
  extends PendingBucketDeletionComponent
  // with OtherComponent
  // with OtherComponent
  {

  this: DriverComponent =>

  import driver.api._

  lazy val allSchemas =
    pendingBucketDeletionQuery.schema
    // ++ otherQuery.schema
    // ++ otherQuery.schema

}
