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
  val batchSize: Int
  
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
      workflowMessageQuery.schema ++
      workflowAuditStatusQuery.schema

  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !

    TableQuery[GroupSubgroupsTable].delete andThen                // FK to group
      TableQuery[GroupUsersTable].delete andThen                  // FK to group, users
      TableQuery[WorkspaceAccessTable].delete andThen             // FK to group, workspace
      TableQuery[EntityAttributeTable].delete andThen             // FK to entity
      TableQuery[WorkspaceAttributeTable].delete andThen          // FK to entity, workspace
      TableQuery[SubmissionAttributeTable].delete andThen         // FK to entity, submissionvalidation
      TableQuery[MethodConfigurationInputTable].delete andThen    // FK to MC
      TableQuery[MethodConfigurationOutputTable].delete andThen   // FK to MC
      TableQuery[MethodConfigurationPrereqTable].delete andThen   // FK to MC
      TableQuery[SubmissionValidationTable].delete andThen        // FK to workflow, workflowfailure
      TableQuery[WorkflowMessageTable].delete andThen             // FK to workflow
      TableQuery[WorkflowTable].delete andThen                    // FK to submission, entity
      TableQuery[WorkflowErrorTable].delete andThen               // FK to workflowfailure
      TableQuery[WorkflowFailureTable].delete andThen             // FK to submission, entity
      TableQuery[SubmissionTable].delete andThen                  // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].delete andThen         // FK to workspace
      TableQuery[ProjectUsersTable].delete andThen                // FK to billingproject, user
      TableQuery[EntityTable].delete andThen                      // FK to workspace
      TableQuery[WorkspaceTable].delete andThen                   // FK to group
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[RawlsGroupTable].delete andThen
      TableQuery[RawlsUserTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete

  }

}
