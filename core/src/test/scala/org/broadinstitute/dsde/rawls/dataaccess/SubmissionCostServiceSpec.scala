package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model.{TableCell, TableRow}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class SubmissionCostServiceSpec extends AnyFlatSpec with RawlsTestUtils {
  implicit val actorSystem = ActorSystem("SubmissionCostServiceSpec")
  val mockBigQueryDAO = new MockGoogleBigQueryDAO
  val submissionCostService = SubmissionCostService.constructor(
    "fakeTableName",
    "fakeDatePartitionColumn",
    "fakeServiceProject",
    31,
    mockBigQueryDAO
  )

  val rows = List(
    new TableRow().setF(
      List(new TableCell().setV("wfKey"), new TableCell().setV("wf1"), new TableCell().setV(1.32f)).asJava
    ),
    new TableRow().setF(
      List(new TableCell().setV("wfKey"), new TableCell().setV("wf2"), new TableCell().setV(3f)).asJava
    ),
    new TableRow().setF(
      List(new TableCell().setV("wfKey"), new TableCell().setV("wf3"), new TableCell().setV(101.00f)).asJava
    )
  ).asJava

  "SubmissionCostService" should "extract a map of workflow ID to cost" in {
    val expected = Map("wf1" -> 1.32f, "wf2" -> 3.00f, "wf3" -> 101.00f)
    assertResult(expected) {
      submissionCostService.extractCostResults(rows)
    }
  }

  it should "return the expected string for generateSubmissionCostsQuery with an existing terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC) // 1970-01-01
    val terminalStatusDate = Option(new DateTime(2020, 10, 9, 13, 31, DateTimeZone.UTC))
    val expectedStartDateString = "1969-12-31" // submissionDate - 1 day
    val expectedEndDateString = "2020-10-10" // terminalStatusDate + 1 day
    val expected =
      s"""SELECT wflabels.key, REPLACE(wflabels.value, "cromwell-", "") as `workflowId`, SUM(billing.cost)
         |FROM `test` as billing, UNNEST(labels) as wflabels
         |CROSS JOIN UNNEST(billing.labels) as blabels
         |WHERE blabels.value = "terra-submission-id"
         |AND wflabels.key = "cromwell-workflow-id"
         |AND project.id = ?
         |AND _PARTITIONDATE BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY wflabels.key, workflowId""".stripMargin
    assertResult(expected) {
      submissionCostService.generateSubmissionCostsQuery("submission-id",
                                                         submissionDate,
                                                         terminalStatusDate,
                                                         "test",
                                                         None
      )
    }
  }

  it should "return the expected string for generateSubmissionCostsQuery with no terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC) // 1970-01-01
    val terminalStatusDate = None
    val expectedStartDateString = "1969-12-31" // submissionDate - 1 day
    val expectedEndDateString = "1970-02-02" // submissionDate + 31 day + 1 day
    val expected =
      s"""SELECT wflabels.key, REPLACE(wflabels.value, "cromwell-", "") as `workflowId`, SUM(billing.cost)
         |FROM `test` as billing, UNNEST(labels) as wflabels
         |CROSS JOIN UNNEST(billing.labels) as blabels
         |WHERE blabels.value = "terra-submission-id"
         |AND wflabels.key = "cromwell-workflow-id"
         |AND project.id = ?
         |AND _PARTITIONDATE BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY wflabels.key, workflowId""".stripMargin
    assertResult(expected) {
      submissionCostService.generateSubmissionCostsQuery("submission-id",
                                                         submissionDate,
                                                         terminalStatusDate,
                                                         "test",
                                                         None
      )
    }
  }

  it should "return the expected string for generateWorkflowCostsQuery with an existing terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC) // 1970-01-01
    val terminalStatusDate = Option(new DateTime(2020, 10, 9, 13, 31, DateTimeZone.UTC))
    val expectedStartDateString = "1969-12-31" // submissionDate - 1 day
    val expectedEndDateString = "2020-10-10" // terminalStatusDate + 1 day
    val expected =
      s"""SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
         |FROM `test`, UNNEST(labels) as labels
         |WHERE project.id = ?
         |AND labels.key LIKE "cromwell-workflow-id"
         |AND _PARTITIONDATE BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY labels.key, workflowId
         |HAVING some having clause""".stripMargin
    assertResult(expected) {
      submissionCostService.generateWorkflowCostsQuery(submissionDate,
                                                       terminalStatusDate,
                                                       "some having clause",
                                                       "test",
                                                       None
      )
    }
  }

  it should "return the expected string for generateWorkflowCostsQuery with no terminal status date input" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC) // 1970-01-01
    val terminalStatusDate = None
    val expectedStartDateString = "1969-12-31" // submissionDate - 1 day
    val expectedEndDateString = "1970-02-02" // submissionDate + 31 day + 1 day
    val expected =
      s"""SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
         |FROM `test`, UNNEST(labels) as labels
         |WHERE project.id = ?
         |AND labels.key LIKE "cromwell-workflow-id"
         |AND _PARTITIONDATE BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY labels.key, workflowId
         |HAVING some having clause""".stripMargin
    assertResult(expected) {
      submissionCostService.generateWorkflowCostsQuery(submissionDate,
                                                       terminalStatusDate,
                                                       "some having clause",
                                                       "test",
                                                       None
      )
    }
  }

  it should "use the custom date partition column name if specified" in {
    val submissionDate = new DateTime(0, DateTimeZone.UTC) // 1970-01-01
    val terminalStatusDate = None
    val expectedStartDateString = "1969-12-31" // submissionDate - 1 day
    val expectedEndDateString = "1970-02-02" // submissionDate + 31 day + 1 day
    val expected =
      s"""SELECT labels.key, REPLACE(labels.value, "cromwell-", "") as `workflowId`, SUM(cost)
         |FROM `test`, UNNEST(labels) as labels
         |WHERE project.id = ?
         |AND labels.key LIKE "cromwell-workflow-id"
         |AND custom_date_partition BETWEEN "$expectedStartDateString" AND "$expectedEndDateString"
         |GROUP BY labels.key, workflowId
         |HAVING some having clause""".stripMargin
    assertResult(expected) {
      submissionCostService.generateWorkflowCostsQuery(
        submissionDate,
        terminalStatusDate,
        "some having clause",
        "test",
        Some("custom_date_partition")
      )
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
      Await.result(
        submissionCostService.getSubmissionCosts("submission-id",
                                                 Seq.empty,
                                                 GoogleProjectId("test"),
                                                 new DateTime(DateTimeZone.UTC),
                                                 Option(new DateTime(DateTimeZone.UTC))
        ),
        1 minute
      )
    }
  }
}
