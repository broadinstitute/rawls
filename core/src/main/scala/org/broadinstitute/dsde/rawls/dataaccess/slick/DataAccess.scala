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
  with ManagedGroupComponent
  with ExprEvalComponent
  with SlickExpressionParser {

  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int
  
  import driver.api._

  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first

    TableQuery[GroupSubgroupsTable].delete andThen                // FK to group
      TableQuery[GroupUsersTable].delete andThen                  // FK to group, users
      TableQuery[WorkspaceAccessTable].delete andThen             // FK to group, workspace
      TableQuery[RawlsBillingProjectGroupTable].delete andThen    // FK to group, billingproject
      TableQuery[EntityAttributeTable].delete andThen             // FK to entity
      TableQuery[WorkspaceAttributeTable].delete andThen          // FK to entity, workspace
      TableQuery[SubmissionAttributeTable].delete andThen         // FK to entity, submissionvalidation
      TableQuery[MethodConfigurationInputTable].delete andThen    // FK to MC
      TableQuery[MethodConfigurationOutputTable].delete andThen   // FK to MC
      TableQuery[MethodConfigurationPrereqTable].delete andThen   // FK to MC
      TableQuery[SubmissionValidationTable].delete andThen        // FK to workflow, workflowfailure
      TableQuery[WorkflowMessageTable].delete andThen             // FK to workflow
      TableQuery[WorkflowTable].delete andThen                    // FK to submission, entity
      TableQuery[SubmissionTable].delete andThen                  // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].delete andThen         // FK to workspace
      TableQuery[EntityTable].delete andThen                      // FK to workspace
      TableQuery[PendingWorkspaceAccessTable].delete andThen      // FK to workspace, user
      TableQuery[WorkspaceUserShareTable].delete andThen          // FK to workspace, user
      TableQuery[WorkspaceGroupShareTable].delete andThen         // FK to workspace, group
      TableQuery[WorkspaceUserCatalogTable].delete andThen        // FK to workspace, user
      TableQuery[WorkspaceGroupCatalogTable].delete andThen       // FK to workspace, group
      TableQuery[WorkspaceTable].delete andThen                   // FK to realm
      TableQuery[ManagedGroupTable].delete andThen                       // FK to group
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[RawlsGroupTable].delete andThen
      TableQuery[RawlsUserTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[SubmissionAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[EntityAttributeScratchTable].delete andThen
      TableQuery[WorkspaceAttributeScratchTable].delete andThen
      TableQuery[ExprEvalScratch].delete
  }

}
