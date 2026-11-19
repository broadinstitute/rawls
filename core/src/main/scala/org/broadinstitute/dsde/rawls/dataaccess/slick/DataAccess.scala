package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.entities.local.LocalEntityExpressionQueries
import slick.jdbc.JdbcProfile

import javax.naming.NameNotFoundException
import javax.naming.directory.DirContext
import scala.util.Try

trait DataAccess
    extends PendingBucketDeletionComponent
    with RawlsBillingProjectComponent
    with WorkspaceComponent
    with EntityComponent
    with CompactEntityComponent
    with CompactEntitySlickComponent
    with AttributeComponent
    with MethodConfigurationComponent
    with SubmissionComponent
    with WorkflowComponent
    with WorkflowActualCostComponent
    with ExprEvalComponent
    with WorkspaceRequesterPaysComponent
    with LocalEntityExpressionQueries
    with CloneWorkspaceFileTransferComponent
    with WorkspaceFeatureFlagComponent
    with WorkspaceManagerResourceMonitorRecordComponent
    with FastPassGrantComponent
    with WorkspaceSettingComponent
    with GoogleProjectRegistrationComponent
    with WorkspaceSpendReportComponent {

  this: DriverComponent =>

  val driver: JdbcProfile
  val batchSize: Int // used for writes to group inserts/updates; must be explicitly utilized via custom business logic
  val fetchSize
    : Int // used during Slick streaming to set the size of pages; must be explicitly set via withStatementParameters

  import driver.api._

  // only called from TestDriverComponent
  def truncateAll: WriteAction[Int] =
    // important to keep the right order for referential integrity !
    // if table X has a Foreign Key to table Y, delete table X first
    // davidan: instead of specific ordering, could we instead SET FOREIGN_KEY_CHECKS = 0; truncate tables ...; SET FOREIGN_KEY_CHECKS = 1; ?

    TableQuery[WorkspaceAttributeTable].delete andThen // FK to entity, workspace
      TableQuery[SubmissionAttributeTable].delete andThen // FK to entity, submissionvalidation
      TableQuery[MethodConfigurationInputTable].delete andThen // FK to MC
      TableQuery[MethodConfigurationOutputTable].delete andThen // FK to MC
      TableQuery[SubmissionValidationTable].delete andThen // FK to workflow, workflowfailure
      TableQuery[WorkflowMessageTable].delete andThen // FK to workflow
      TableQuery[WorkflowActualCostTable].delete andThen
      TableQuery[WorkflowTable].delete andThen // FK to submission, entity
      TableQuery[SubmissionTable].delete andThen // FK to workspace, user, MC, entity
      TableQuery[MethodConfigurationTable].delete andThen // FK to workspace
      TableQuery[EntityTable].delete andThen // FK to workspace
      TableQuery[WorkspaceRequesterPaysTable].delete andThen // FK to workspace
      TableQuery[CloneWorkspaceFileTransferTable].delete andThen // FK to workspace
      TableQuery[WorkspaceTable].delete andThen
      TableQuery[BillingAccountChanges].delete andThen // FK to BillingProject
      TableQuery[GoogleProjectRegistrationTable].delete andThen // FK to BillingProject
      TableQuery[RawlsBillingProjectTable].delete andThen
      TableQuery[PendingBucketDeletionTable].delete andThen
      TableQuery[WorkspaceAttributeTempTable].delete andThen
      TableQuery[ExprEvalScratch].delete andThen
      sql"delete from ATTRIBUTE_CORRECTIONS".asUpdate andThen
      sql"delete from ENTITY_CORRECTIONS".asUpdate

  def sqlDBStatus() =
    sql"select version()".as[String]

  private def clear(ctx: DirContext, dn: String): Unit = Try {
    import scala.jdk.CollectionConverters._
    ctx.list(dn).asScala.foreach { nameClassPair =>
      val fullName = if (nameClassPair.isRelative) s"${nameClassPair.getName},$dn" else nameClassPair.getName
      clear(ctx, fullName)
      ctx.unbind(fullName)
    }
  } recover { case _: NameNotFoundException =>
  }
}
