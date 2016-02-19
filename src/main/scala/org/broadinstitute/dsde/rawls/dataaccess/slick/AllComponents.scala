package org.broadinstitute.dsde.rawls.dataaccess.slick

trait AllComponents
  extends PendingBucketDeletionComponent
  with RawlsUserComponent
  with RawlsGroupComponent
  with RawlsBillingProjectComponent
  with WorkspaceComponent
  with EntityComponent
  with AttributeComponent
  with MethodConfigurationComponent {

  this: DriverComponent =>

  import driver.api._

  lazy val allSchemas = 
    pendingBucketDeletionQuery.schema ++
    rawlsUserQuery.schema ++
    rawlsGroupQuery.schema ++
    rawlsBillingProjectQuery.schema ++
    groupUsersQuery.schema ++
    groupSubgroupsQuery.schema ++
    projectUsersQuery.schema ++
    attributeQuery.schema ++
    workspaceQuery.schema ++
    workspaceAttributeQuery.schema ++
    workspaceAccessQuery.schema ++
    entityQuery.schema ++
    entityAttributeQuery.schema ++
    methodConfigurationQuery.schema ++
    methodConfigurationInputQuery.schema ++
    methodConfigurationOutputQuery.schema ++
    methodConfigurationPrereqQuery.schema
}
