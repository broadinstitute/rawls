package org.broadinstitute.dsde.rawls.dataaccess.slick

import javax.naming.NameNotFoundException
import javax.naming.directory.DirContext
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityExpressionQueries
import slick.jdbc.JdbcProfile

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
  with ExprEvalComponent
  with WorkspaceRequesterPaysComponent
  with EntityTypeStatisticsComponent
  with EntityAttributeStatisticsComponent
  with LocalEntityExpressionQueries {


  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int
  
  import driver.api._

  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first

    TableQuery[EntityAttributeTable].delete andThen             // FK to entity
      TableQuery[WorkspaceAttributeTable].delete andThen          // FK to entity, workspace
      TableQuery[SubmissionAttributeTable].delete andThen         // FK to entity, submissionvalidation
      TableQuery[MethodConfigurationInputTable].delete andThen    // FK to MC
      TableQuery[MethodConfigurationOutputTable].delete andThen   // FK to MC
      TableQuery[SubmissionValidationTable].delete andThen        // FK to workflow, workflowfailure
      TableQuery[WorkflowMessageTable].delete andThen             // FK to workflow
      TableQuery[WorkflowTable].delete andThen                    // FK to submission, entity
      TableQuery[SubmissionTable].delete andThen                  // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].delete andThen         // FK to workspace
      TableQuery[EntityTable].delete andThen                      // FK to workspace
      TableQuery[WorkspaceRequesterPaysTable].delete andThen      // FK to workspace
      TableQuery[EntityTypeStatisticsTable].delete andThen        // FK to workspace
      TableQuery[EntityAttributeStatisticsTable].delete andThen   // FK to workspace
      TableQuery[WorkspaceTable].delete andThen
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[SubmissionAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[EntityAttributeTempTable].delete andThen
      TableQuery[WorkspaceAttributeScratchTable].delete andThen
      TableQuery[ExprEvalScratch].delete
  }

  def sqlDBStatus() = {
    sql"select version()".as[String]
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
