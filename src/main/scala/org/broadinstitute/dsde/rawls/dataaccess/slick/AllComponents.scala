package org.broadinstitute.dsde.rawls.dataaccess.slick

trait AllComponents
  extends PendingBucketDeletionComponent
  with RawlsUserComponent
  with RawlsGroupComponent
  with RawlsBillingProjectComponent
  with UserMembershipComponent
  with SubgroupMembershipComponent
  with ProjectMembershipComponent
  with WorkspaceComponent
  with EntityComponent
  with AttributeComponent {

  this: DriverComponent =>

  import driver.api._

  lazy val allSchemas = pendingBucketDeletionQuery.schema ++
    rawlsUserQuery.schema ++
    rawlsGroupQuery.schema ++
    rawlsBillingProjectQuery.schema ++
    userMembershipQuery.schema ++
    subgroupMembershipQuery.schema ++
    projectMembershipQuery.schema ++
    attributeQuery.schema ++
    workspaceQuery.schema ++
    workspaceAttributeQuery.schema ++
    workspaceAccessQuery.schema ++
    entityQuery.schema ++
    entityAttributeQuery.schema
}