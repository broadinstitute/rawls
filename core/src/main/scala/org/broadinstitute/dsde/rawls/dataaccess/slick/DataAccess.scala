package org.broadinstitute.dsde.rawls.dataaccess.slick

import javax.naming.NameNotFoundException
import javax.naming.directory.DirContext

import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
import org.broadinstitute.dsde.rawls.expressions.SlickExpressionParser
import org.broadinstitute.dsde.rawls.model.{RawlsGroupName, RawlsGroupRef}
import slick.jdbc.JdbcProfile

import scala.concurrent.Future
import scala.util.Try

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
      TableQuery[WorkspaceUserComputeTable].delete andThen          // FK to workspace, user
      TableQuery[WorkspaceGroupComputeTable].delete andThen         // FK to workspace, group
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

  def clearLdap(): Future[Unit] = withContext(directoryConfig.directoryUrl, directoryConfig.user, directoryConfig.password) { ctx =>
    clear(ctx, resourcesOu)
    clear(ctx, groupsOu)
    clear(ctx, peopleOu)
  }

  private def clear(ctx: DirContext, dn: String): Unit = Try {
    import scala.collection.JavaConverters._
    ctx.list(dn).asScala.foreach { nameClassPair =>
      val fullName = if (nameClassPair.isRelative) s"${nameClassPair.getName},$dn" else nameClassPair.getName
      clear(ctx, fullName)
      ctx.unbind(fullName)
    }
  } recover {
    case _: NameNotFoundException =>
  }
}
