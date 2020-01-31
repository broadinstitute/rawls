package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model.{TableCell, TableRow}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

class SubmissionCostServiceSpec extends FlatSpec with RawlsTestUtils {
  implicit val actorSystem = ActorSystem("SubmissionCostServiceSpec")
  val mockBigQueryDAO = new MockGoogleBigQueryDAO
  val submissionCostService = SubmissionCostService.constructor("test", "test", mockBigQueryDAO)

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

  /*
    `MockGoogleBigQueryDAO` will throw an exception if the parameters passed to startParameterizedQuery
    are not equal to the fields `testProject`, `testParamQuery`, `testParameters` and `testParameterMode`.
    `SubmissionCostService#executeWorkflowCostQuery` passes other values to `startParameterizedQuery`, so
    an exception will be thrown if a call is made to BigQuery in this case.
   */
  it should "bypass BigQuery with no workflow IDs" in {
    assertResult(Map.empty) {
      Await.result(submissionCostService.getSubmissionCosts("submission-id", Seq.empty, "test", None), 1 minute)
    }
  }
}
