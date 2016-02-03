package org.broadinstitute.dsde.rawls.dataaccess.slick

trait AllComponents
  extends PendingBucketDeletionComponent
    with RawlsUserComponent
    with RawlsGroupComponent
    with RawlsBillingProjectComponent {

  this: DriverComponent =>

  import driver.api._

  lazy val allSchemas =
    pendingBucketDeletionQuery.schema ++
    rawlsUserQuery.schema ++
    rawlsGroupQuery.schema ++
    rawlsBillingProjectQuery.schema
}
