package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser
import slick.driver.JdbcProfile

trait DataAccess
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
    
  val driver: JdbcProfile
  
  import driver.api._

  lazy val allSchemas =
    pendingBucketDeletionQuery.schema ++
      rawlsUserQuery.schema ++
      rawlsGroupQuery.schema ++
      rawlsBillingProjectQuery.schema ++
      groupUsersQuery.schema ++
      groupSubgroupsQuery.schema ++
      projectUsersQuery.schema ++
      workspaceQuery.schema ++
      workspaceAccessQuery.schema ++
      workspaceAttributeQuery.schema ++
      entityQuery.schema ++
      entityAttributeQuery.schema ++
      methodConfigurationQuery.schema ++
      methodConfigurationInputQuery.schema ++
      methodConfigurationOutputQuery.schema ++
      methodConfigurationPrereqQuery.schema ++
      submissionQuery.schema ++
      submissionAttributeQuery.schema ++
      submissionValidationQuery.schema ++
      workflowQuery.schema ++
      workflowErrorQuery.schema ++
      workflowFailureQuery.schema ++
      workflowMessageQuery.schema
}
