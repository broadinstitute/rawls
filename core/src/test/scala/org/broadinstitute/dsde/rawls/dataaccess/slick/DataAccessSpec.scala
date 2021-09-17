package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable

import scala.concurrent.Future

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers with ScalaFutures {

  import driver.api._

  behavior of "DataAccess"

  val rawTableNames: Seq[String] = runAndWait(MTable.getTables).map(_.name.name)

  val safeTableNames: Seq[String] = rawTableNames flatMap {
    case "GROUP" => None
    case "USER" => None
    case "MANAGED_GROUP" => None
    case "shards" => None // populated by Liquibase, not written to by Rawls
    case "ENTITY_ATTRIBUTE_archived" => None // never read by Rawls, will be deleted
    case "DATABASECHANGELOG" => None        // managed by Liquibase
    case "DATABASECHANGELOGLOCK" => None    // managed by Liquibase
    case other => Option(other)
  }

  safeTableNames foreach { tableName =>
    it should s"test that truncateAll has left table '$tableName' in a known empty state" in withEmptyTestDatabase {
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
    val altDataSource = new SlickDataSource(altDataConfig)(TestExecutionContext.testExecutionContext)

    withCustomTestDatabaseInternal(altDataSource, testData) {
      withWorkspaceContext(testData.workspace) { context =>

        // needs to be >> than thread count
        val roundtripCheckActions = (1 to 100).map { _ =>
          val submissionId = UUID.randomUUID().toString
          val testSubmission = testData.submissionUpdateEntity.copy(submissionId = submissionId)

          for {
            insert <- submissionQuery.create(context, testSubmission)
            select <- submissionQuery.get(context, submissionId)
          } yield {
            insert shouldBe testSubmission
            select shouldBe Some(testSubmission)
          }
        }

        // execute the actions in concurrent transactions and wait for them
        // can't use runAndWait here because we need to use our altDataSource

        val roundtripCheckFutures = roundtripCheckActions map { a => altDataSource.inTransaction { _ => a } }
        Future.sequence(roundtripCheckFutures).futureValue(timeout(Span(10, Seconds)))
      }
    }
  }

  it should "use utf8 for MySQL's character_set_server" in withEmptyTestDatabase {
    /* Our live-environment CloudSQL instances, including production, use character_set_server=utf8.
       This setting is critical for SQL queries that specify/override a collation, such as to make a
       case-sensitive query against a column that is case-insensitive by default, e.g. the column
       uses utf8_general_ci collation.

       A failure of this test likely means that our test environment is not set up correctly; the mysql
       instance used by tests does not have the right setting. If the test-instance mysql is not set up
       correctly, other tests can fail and/or return false positives.
     */
    val charsetLookup = runAndWait(sql"""SHOW VARIABLES WHERE Variable_name = 'character_set_server';""".as[(String, String)])
    charsetLookup should have size 1
    withClue("is the mysql against which these unit tests ran set up correctly with --character-set-server=utf8 or equivalent?") {
      charsetLookup.head._2 shouldBe "utf8"
    }
  }
}
