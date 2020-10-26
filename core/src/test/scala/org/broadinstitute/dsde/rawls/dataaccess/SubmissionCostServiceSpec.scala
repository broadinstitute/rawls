package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model.{TableCell, TableRow}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

class SubmissionCostServiceSpec extends FlatSpec with RawlsTestUtils {
  implicit val actorSystem = ActorSystem("SubmissionCostServiceSpec")
  val mockBigQueryDAO = new MockGoogleBigQueryDAO
  val submissionCostService = SubmissionCostService.constructor("test", "test", 31, mockBigQueryDAO)

  val rows = List(
    new TableRow().setF(List(new TableCell().setV("wfKey"), new TableCell().setV("wf1"), new TableCell().setV(1.32f)).asJava),
    new TableRow().setF(List(new TableCell().setV("wfKey"), new TableCell().setV("wf2"), new TableCell().setV(3f)).asJava),
    new TableRow().setF(List(new TableCell().setV("wfKey"), new TableCell().setV("wf3"), new TableCell().setV(101.00f)).asJava)
  ).asJava

  "SubmissionCostService" should "extract a map of workflow ID to cost" in {
    val expected = Map("wf1" -> 1.32f, "wf2" -> 3.00f, "wf3" -> 101.00f)
    assertResult(expected) {
      submissionCostService.extractCostResults(rows)
    }
  }

  it should "return the expected string for generateSubmissionCostsQuery with an existing terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC)  // 1970-01-01
    val terminalStatusDate = Option(new DateTime(2020, 10, 9, 13, 31, DateTimeZone.UTC))
    val expectedStartDateString = "1969-12-31"  // submissionDate - 1 day
    val expectedEndDateString = "2020-10-10"  // terminalStatusDate + 1 day
    val expected =
      s"""SELECT 'cromwell-workflow-id', workflow_id, SUM(cost)
         |FROM `test`
         |WHERE submission_id = 'test-submission-id'
         |AND billing_date BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY 1,2""".stripMargin    
    assertResult(expected) {
      submissionCostService.generateSubmissionCostsQuery("test-submission-id", submissionDate, terminalStatusDate)
    }
  }

  it should "return the expected string for generateSubmissionCostsQuery with no terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC)  // 1970-01-01
    val terminalStatusDate = None
    val expectedStartDateString = "1969-12-31"  // submissionDate - 1 day
    val expectedEndDateString = "1970-02-02"  // submissionDate + 31 day + 1 day
    val expected =
      s"""SELECT 'cromwell-workflow-id', workflow_id, SUM(cost)
         |FROM `test`
         |WHERE submission_id = 'test-submission-id'
         |AND billing_date BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY 1,2""".stripMargin    
    assertResult(expected) {
      submissionCostService.generateSubmissionCostsQuery("test-submission-id", submissionDate, terminalStatusDate)
    }
  }

  it should "return the expected string for generateWorkflowCostsQuery with an existing terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC)  // 1970-01-01
    val terminalStatusDate = Option(new DateTime(2020, 10, 9, 13, 31, DateTimeZone.UTC))
    val expectedStartDateString = "1969-12-31"  // submissionDate - 1 day
    val expectedEndDateString = "2020-10-10"  // terminalStatusDate + 1 day
    val expected =
      s"""SELECT 'cromwell-workflow-id', workflow_id, SUM(cost)
         |FROM `test`
         |WHERE workflow_id IN (?, ?, ?)
         |AND billing_date BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY 1,2""".stripMargin    
    assertResult(expected) {
      submissionCostService.generateWorkflowCostsQuery(submissionDate, terminalStatusDate, 3)
    }
  }

  it should "return the expected string for generateWorkflowCostsQuery with no terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC)  // 1970-01-01
    val terminalStatusDate = None
    val expectedStartDateString = "1969-12-31"  // submissionDate - 1 day
    val expectedEndDateString = "1970-02-02"  // submissionDate + 31 day + 1 day
    val expected =
      s"""SELECT 'cromwell-workflow-id', workflow_id, SUM(cost)
         |FROM `test`
         |WHERE workflow_id IN (?, ?, ?)
         |AND billing_date BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY 1,2""".stripMargin    
    assertResult(expected) {
      submissionCostService.generateWorkflowCostsQuery(submissionDate, terminalStatusDate, 3)
    }
  }

  /*
    `MockGoogleBigQueryDAO` will throw an exception if the parameters passed to startParameterizedQuery
    are not equal to the fields `testProject`, `testParamQuery`, `testParameters` and `testParameterMode`.
    `SubmissionCostService#executeWorkflowCostQuery` passes other values to `startParameterizedQuery`, so
    an exception will be thrown if a call is made to BigQuery in this case.
   */
  it should "bypass BigQuery with no workflow IDs" in {
    assertResult(Map.empty) {
      Await.result(submissionCostService.getSubmissionCosts("submission-id", Seq.empty, "test", new DateTime(DateTimeZone.UTC), Option(new DateTime(DateTimeZone.UTC))), 1 minute)
    }
  }
}
