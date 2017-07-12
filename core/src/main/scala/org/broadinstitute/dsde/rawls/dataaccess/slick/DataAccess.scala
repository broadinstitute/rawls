package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser
import slick.driver.JdbcProfile
import slick.jdbc.SQLActionBuilder
import slick.jdbc.SetParameter.SetUnit
import slick.lifted.AbstractTable

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

    TableQuery[GroupSubgroupsTable].truncate andThen                // FK to group
      TableQuery[GroupUsersTable].truncate andThen                  // FK to group, users
      TableQuery[WorkspaceAccessTable].truncate andThen             // FK to group, workspace
      TableQuery[RawlsBillingProjectGroupTable].truncate andThen    // FK to group, billingproject
      TableQuery[EntityAttributeTable].truncate andThen             // FK to entity
      TableQuery[WorkspaceAttributeTable].truncate andThen          // FK to entity, workspace
      TableQuery[SubmissionAttributeTable].truncate andThen         // FK to entity, submissionvalidation
      TableQuery[MethodConfigurationInputTable].truncate andThen    // FK to MC
      TableQuery[MethodConfigurationOutputTable].truncate andThen   // FK to MC
      TableQuery[MethodConfigurationPrereqTable].truncate andThen   // FK to MC
      TableQuery[SubmissionValidationTable].truncate andThen        // FK to workflow, workflowfailure
      TableQuery[WorkflowMessageTable].truncate andThen             // FK to workflow
      TableQuery[WorkflowTable].truncate andThen                    // FK to submission, entity
      TableQuery[SubmissionTable].truncate andThen                  // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].truncate andThen         // FK to workspace
      TableQuery[EntityTable].truncate andThen                      // FK to workspace
      TableQuery[PendingWorkspaceAccessTable].truncate andThen      // FK to workspace, user
      TableQuery[WorkspaceUserShareTable].truncate andThen          // FK to workspace, user
      TableQuery[WorkspaceGroupShareTable].truncate andThen         // FK to workspace, group
      TableQuery[WorkspaceUserCatalogTable].truncate andThen        // FK to workspace, user
      TableQuery[WorkspaceGroupCatalogTable].truncate andThen       // FK to workspace, group
      TableQuery[WorkspaceTable].truncate andThen                   // FK to realm
      TableQuery[ManagedGroupTable].truncate andThen                // FK to group
      TableQuery[RawlsBillingProjectTable].truncate andThen
      TableQuery[RawlsGroupTable].truncate andThen
      TableQuery[RawlsUserTable].truncate andThen
      TableQuery[WorkflowAuditStatusTable].truncate andThen
      TableQuery[SubmissionAuditStatusTable].truncate andThen
      TableQuery[PendingBucketDeletionTable].truncate andThen
      TableQuery[EntityAttributeScratchTable].truncate andThen
      TableQuery[WorkspaceAttributeScratchTable].truncate andThen
      TableQuery[ExprEvalScratch].truncate
  }

  implicit class TruncateTableSupport[A <: AbstractTable[_]](tq: TableQuery[A]) {
    def truncate: WriteAction[Int] = {
      val trunc = SQLActionBuilder(List(s"TRUNCATE TABLE `${tq.baseTableRow.tableName}`"), SetUnit).asUpdate
      // need to temporarily disable foreign key checks for the truncate command
      DBIO.sequence(
        Seq(
          sqlu"""SET FOREIGN_KEY_CHECKS = 0;""",
          trunc,
          sqlu"""SET FOREIGN_KEY_CHECKS = 1;"""
        )
      ).map(results => results(1)) // return the result of the truncate command
    }
  }

  def sqlDBStatus() = {
    sql"select version()".as[String]
  }

}
