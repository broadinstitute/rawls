package org.broadinstitute.dsde.rawls.dataaccess.slick

import javax.naming.NameNotFoundException
import javax.naming.directory.DirContext
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityExpressionQueries
import org.broadinstitute.dsde.rawls.model.WorkspaceShardStates
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable

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

  // pre-calculate all shard identifiers, by generating all possible combinations of the first two chars of a UUID,
  // then calling determineShard on those UUIDs. This allows determineShard to change without having to change the
  // implementation here.
  val allShards = ((0L to 15L) flatMap { firstLong =>
    (0L to 15L) map { secondLong =>
      val first = firstLong.toHexString
      val second = secondLong.toHexString
      determineShard(java.util.UUID.fromString(s"$first${second}000000-0000-0000-0000-000000000000"), shardState = WorkspaceShardStates.Sharded).toString
    }
  }).toSet + determineShard(java.util.UUID.fromString(s"00000000-0000-0000-0000-000000000000"), shardState = WorkspaceShardStates.Unsharded).toString

  // only called from TestDriverComponent
  def truncateAll: WriteAction[Int] = {
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first
    // davidan: instead of specific ordering, could we instead SET FOREIGN_KEY_CHECKS = 0; truncate tables ...; SET FOREIGN_KEY_CHECKS = 1; ?

    val shardDeletes = allShards.map { shardSuffix =>
      new EntityAttributeShardQuery(shardSuffix).delete
    }.toSeq

    DBIO.sequence(shardDeletes) andThen // FK to entity
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
