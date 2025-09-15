package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.instances.int._
import cats.instances.list._
import cats.instances.map._
import cats.syntax.foldable._
import nl.grons.metrics4.scala.Counter
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.ExecutionServiceId
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.Write
import slick.jdbc.{GetResult, JdbcProfile, SQLActionBuilder}

import java.sql.Timestamp
import java.util.UUID

case class WorkflowActualCostRecord(externalId: String, cost: Option[Float])

trait WorkflowActualCostComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkflowActualCostTable(tag: Tag) extends Table[WorkflowActualCostRecord](tag, "WORKFLOW_ACTUAL_COST") {
    def externalId = column[String]("EXTERNAL_ID", O.SqlType("CHAR(36)"))
    def cost = column[Option[Float]]("COST")

    def * = (
      externalId,
      cost
    ) <> (WorkflowActualCostRecord.tupled, WorkflowActualCostRecord.unapply)
  }

  object workflowActualCostQuery extends TableQuery(new WorkflowActualCostTable(_)) {
    type WorkflowActualCostQueryType = Query[WorkflowActualCostTable, WorkflowRecord, Seq]
  }

  object workflowActualCostRawSqlQuery extends RawSqlQuery {
    val driver: JdbcProfile = WorkflowActualCostComponent.this.driver

    /**
     * Insert rows into WORKFLOW_ACTUAL_COST, ignoring any conflicts on duplicate EXTERNAL_ID values.
     * Because this method may be called concurrently by multiple users for the same workflows,
     * it can try to insert the same rows multiple times. These rows are a cache of the value
     * already in BigQuery, which should not change - therefore, we can ignore any duplicates here
     * on the assumption they will have the same values.
     *
     * @param rows the rows to insert
     * @return count of rows affected
     */
    def safeInsert(rows: Seq[WorkflowActualCostRecord]): ReadWriteAction[Int] = {

      // generate parameters for each row
      val rowParams: Seq[SQLActionBuilder] = rows.map { row =>
        sql"""(${row.externalId}, ${row.cost})"""
      }

      val paramSql = reduceSqlActionsWithDelim(rowParams, sql",")

      val startSql = sql"""
            insert into WORKFLOW_ACTUAL_COST(EXTERNAL_ID, COST)
            values """
      val endSql = sql""" on duplicate key update COST=COST;"""

      concatSqlActions(startSql, paramSql, endSql).asUpdate
    }

  }

}
