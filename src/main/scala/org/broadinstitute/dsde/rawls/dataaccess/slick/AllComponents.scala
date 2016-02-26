package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser

trait AllComponents
  extends PendingBucketDeletionComponent
  with RawlsUserComponent
  with RawlsGroupComponent
  with RawlsBillingProjectComponent
  with WorkspaceComponent
  with EntityComponent
  with AttributeComponent
  with MethodConfigurationComponent
  with SubmissionComponent
  with WorkflowComponent
  with SlickExpressionParser {

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
    methodConfigurationPrereqQuery.schema ++
    submissionQuery.schema ++
    submissionValidationQuery.schema ++
    workflowQuery.schema ++
    workflowErrorQuery.schema ++
    workflowFailureQuery.schema ++
    workflowMessageQuery.schema
}
