package org.broadinstitute.dsde.rawls.dataaccess.slick

import javax.naming.NameNotFoundException
import javax.naming.directory.DirContext
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityExpressionQueries
import slick.jdbc.JdbcProfile

import java.lang.Integer.toHexString
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
  with EntityCacheComponent
  with LocalEntityExpressionQueries
  with CloneWorkspaceFileTransferComponent {


  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int

  import driver.api._

  // only called from TestDriverComponent
  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first

    // TODO: davidan could we instead SET FOREIGN_KEY_CHECKS = 0; truncate tables ...; SET FOREIGN_KEY_CHECKS = 1; ?
    // do we have access to INFORMATION_SCHEMA.TABLES, and if so can we avoid listing all tables here?
    val shardDeletes = DBIO.sequence((0 to 15).map(toHexString) flatMap { firstPart =>
      List("07", "8f") map { secondPart =>
        val shardSuffix = firstPart + "_" + secondPart
        new EntityAttributeShardQuery(shardSuffix).delete
      }
    })

    // TODO: davidan don't hardcode the shard names here, else this will need to be in sync with liquibase
    shardDeletes andThen // FK to entity
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
      TableQuery[EntityCacheTable].delete andThen                 // FK to workspace
      TableQuery[CloneWorkspaceFileTransferTable].delete andThen   // FK to workspace
      TableQuery[WorkspaceTable].delete andThen
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[WorkflowAuditStatusTable].delete andThen
      TableQuery[SubmissionAuditStatusTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[EntityAttributeTempTable].delete andThen
      TableQuery[WorkspaceAttributeTempTable].delete andThen
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
