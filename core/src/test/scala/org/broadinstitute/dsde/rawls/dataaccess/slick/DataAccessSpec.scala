package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.DbResource.dirConfig
import org.broadinstitute.dsde.rawls.model.{Workflow, WorkflowStatuses}
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration.Interval
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable

import scala.collection.immutable
import scala.concurrent.Future

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers with ScalaFutures {

  import driver.api._

  "DataAccess" should "test that truncateAll has left the DB in a known empty state" in withEmptyTestDatabase {
    val rawTableNames: Seq[String] = runAndWait(MTable.getTables).map(_.name.name)

    val safeTableNames: Seq[String] = rawTableNames flatMap {
      case "GROUP" => None
      case "USER" => None
      case "DATABASECHANGELOG" => None        // managed by Liquibase
      case "DATABASECHANGELOGLOCK" => None    // managed by Liquibase
      case other => Option(other)
    }

    safeTableNames foreach { tableName =>
      val count = sql"SELECT COUNT(*) FROM #$tableName "
      assertResult(0, tableName + " not empty") {
        runAndWait(count.as[Int].head)
      }
    }
  }

  // The following test requires quickly repeated reads and writes from a table whose PK is the FK to another table.
  it should "not deadlock due to too few threads" in {
    // DB Config with only 2 threads
    val altDataConfig = DatabaseConfig.forConfig[JdbcProfile]("mysql-low-thread-count")
    val altDataSource = new SlickDataSource(altDataConfig, dirConfig)(TestExecutionContext.testExecutionContext)

    withCustomTestDatabaseInternal(altDataSource, testData) {
      withWorkspaceContext(testData.workspace) { context =>
        val testSubmission = testData.submissionUpdateEntity

        // needs to be >> than thread count
        val roundtripCheckActions = (1 to 100).map { _ =>
          val wfid = UUID.randomUUID().toString
          val workflow = Workflow(Option(wfid), WorkflowStatuses.Queued, testDate, testSubmission.submissionEntity, testData.inputResolutions)

          for {
            insert <- workflowQuery.createWorkflows(context, UUID.fromString(testSubmission.submissionId), Seq(workflow))
            select <- workflowQuery.getByExternalId(wfid, testSubmission.submissionId)
          } yield {
            insert shouldBe Seq(workflow)
            select shouldBe Some(workflow)
          }
        }

        // execute the actions in concurrent transactions and wait for them
        // can't use runAndWait here because we need to use our altDataSource

        val roundtripCheckFutures = roundtripCheckActions map { a => altDataSource.inTransaction { _ => a } }
        Future.sequence(roundtripCheckFutures).futureValue(Interval(Span(10, Seconds)))
      }
    }
  }
}
