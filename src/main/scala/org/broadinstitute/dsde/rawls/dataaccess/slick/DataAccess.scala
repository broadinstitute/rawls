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
      TableQuery[SubmissionTable].delete andThen                  // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].delete andThen         // FK to workspace
      TableQuery[ProjectUsersTable].delete andThen                // FK to billingproject, user
      TableQuery[EntityTable].delete andThen                      // FK to workspace
      TableQuery[WorkspaceTable].delete andThen                   // FK to group
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[RawlsGroupTable].delete andThen
      TableQuery[RawlsUserTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[SubmissionAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[EntityAttributeTempTable].delete andThen
      TableQuery[WorkspaceAttributeTempTable].delete andThen
      TableQuery[ExprEvalTemp].delete
  }

}
