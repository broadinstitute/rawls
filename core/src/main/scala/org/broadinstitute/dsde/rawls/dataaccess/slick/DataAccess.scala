package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser
import org.broadinstitute.dsde.rawls.model.{RawlsGroupName, RawlsGroupRef}
import slick.driver.JdbcProfile

trait DataAccess
  extends PendingBucketDeletionComponent
  with RawlsBillingProjectComponent
  with WorkspaceComponent
  with EntityComponent
  with AttributeComponent
  with MethodConfigurationComponent
  with SubmissionComponent
  with WorkflowComponent
  with ManagedGroupComponent
  with ExprEvalComponent
  with SlickExpressionParser
  with JndiDirectoryDAO {


  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int
  
  import driver.api._

  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first

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
      TableQuery[WorkspaceAuthDomainTable].delete andThen         // FK to workspace, managed group
      TableQuery[WorkspaceTable].delete andThen
      TableQuery[ManagedGroupTable].delete andThen                // FK to group
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[SubmissionAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[EntityAttributeScratchTable].delete andThen
      TableQuery[WorkspaceAttributeScratchTable].delete andThen
      TableQuery[ExprEvalScratch].delete
  }

  def sqlDBStatus() = {
    sql"select version()".as[String]
  }

  def emptyLdap = {
    // user load all users to read in all users,
    // delete each user
    // load all groups, delete each one
    withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password) {ctx =>
      rawlsUserQuery.loadAllUsers.map{users =>
        users.map{user => ctx.unbind(user.userSubjectId.toString)}
      }
      rawlsGroupQuery.loadAllGroups.map{groups =>
        groups.map{group => ctx.unbind(group.groupName.toString)}
      }
    }
  }

}
