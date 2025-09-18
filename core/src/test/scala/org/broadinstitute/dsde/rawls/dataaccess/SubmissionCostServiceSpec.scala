package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.services.bigquery.model.{
  GetQueryResultsResponse,
  Job,
  JobReference,
  QueryParameter,
  QueryParameterType,
  QueryParameterValue,
  TableCell,
  TableRow
}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowActualCostRecord
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.joda.time.{DateTime, DateTimeZone}
import org.mockito.ArgumentCaptor
import org.scalatest.flatspec.AnyFlatSpec
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, spy, times, verify, when}

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

class SubmissionCostServiceSpec extends AnyFlatSpec with RawlsTestUtils with MockitoTestUtils {
  implicit val actorSystem: ActorSystem = ActorSystem("SubmissionCostServiceSpec")
  val mockBigQueryDAO = new MockGoogleBigQueryDAO
  val submissionCostService = SubmissionCostServiceImpl.constructor(
    "fakeTableName",
    "fakeDatePartitionColumn",
    "fakeServiceProject",
    31,
    slickDataSource,
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
  it should "bypass BigQuery with no workflow IDs" in
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

  behavior of "actual-cost caching"

  // helper: the Google project id used for all tests
  val defaultGoogleProjectId = GoogleProjectId("doesnotmatter")
  // helper: create a SubmissionCostServiceImpl using a mockito mock for BigQuery
  def getMockitoSubmissionCostService(bqCosts: Map[String, Float]) = {
    val mockitoGoogleBigQueryDAO = mock[GoogleBigQueryDAO]
    val testJobReference: JobReference = new JobReference().setJobId("test job id")
    val testJob: Job = new Job().setJobReference(testJobReference)
    when(
      mockitoGoogleBigQueryDAO.startParameterizedQuery(any[GoogleProject],
                                                       any[String],
                                                       any[List[QueryParameter]],
                                                       any[String]
      )
    )
      .thenReturn(Future.successful(testJobReference))
    when(mockitoGoogleBigQueryDAO.getQueryStatus(any[JobReference]))
      .thenReturn(Future.successful(testJob))
    returnCostsFrom(mockitoGoogleBigQueryDAO, bqCosts)
    val costService = SubmissionCostServiceImpl.constructor(
      "fakeTableName",
      "fakeDatePartitionColumn",
      "fakeServiceProject",
      31,
      slickDataSource,
      mockitoGoogleBigQueryDAO
    )
    (costService, mockitoGoogleBigQueryDAO)
  }
  // helper: generate a random cost with precision 2
  def randomCost: Float = BigDecimal(Random.nextFloat() * 100)
    .setScale(2, RoundingMode.HALF_UP)
    .toFloat

  // helper: generate some randomized workflow ids and costs
  def generateWorkflowCosts(quantity: Int): Map[String, Float] = Range(0, quantity).map { _ =>
    // ensure floats have precision 2
    UUID.randomUUID().toString -> BigDecimal(Random.nextFloat() * 10)
      .setScale(2, RoundingMode.HALF_UP)
      .toFloat
  }.toMap
  // helper: return the supplied costs for the supplied workflows from mock BigQuery
  def returnCostsFrom(bigQuery: GoogleBigQueryDAO, costs: Map[String, Float]) = {
    val rows = costs
      .map { case (wfid, cost) =>
        new TableRow().setF(
          List(new TableCell().setV("wfKey"), new TableCell().setV(wfid), new TableCell().setV(cost)).asJava
        )
      }
      .toList
      .asJava

    val bqResponse = new GetQueryResultsResponse
    bqResponse.setRows(rows)

    when(bigQuery.getQueryResult(any[Job]))
      .thenReturn(Future.successful(bqResponse))
  }
  // helper:
  def assertWorkflowParams(params: ArgumentCaptor[List[QueryParameter]], workflowIds: Iterable[String]) = {
    val stringParamType = new QueryParameterType().setType("STRING")
    // expected params are the google project id + all workflows
    val googleProjectIdParam = new QueryParameter()
      .setParameterType(stringParamType)
      .setParameterValue(new QueryParameterValue().setValue(defaultGoogleProjectId.value))
    val workflowParams = workflowIds.toList map { workflowId =>
      new QueryParameter()
        .setParameterType(stringParamType)
        .setParameterValue(new QueryParameterValue().setValue(s"%$workflowId%"))
    }
    params.getValue should contain theSameElementsAs (workflowParams ++ Seq(googleProjectIdParam))
  }

  it should "ask BigQuery for all workflows if nothing is cached" in withDefaultTestDatabase { _: SlickDataSource =>
    val submission = testData.costedSubmission1
    // 3 workflows, none cached
    val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
    val uncachedCosts = costs
    // get the mocked service
    val (costService, bigQuery) = getMockitoSubmissionCostService(uncachedCosts)
    // execute getSubmissionCosts
    val actual = Await.result(
      costService.getSubmissionCosts(
        submission.submissionId,
        costs.keySet.toSeq,
        defaultGoogleProjectId,
        DateTime.now().minusDays(7),
        Option(DateTime.now().minusDays(5))
      ),
      Duration.Inf
    )
    // verify correct results
    actual shouldBe costs
    // verify call to BigQuery
    val paramsCaptor = captor[List[QueryParameter]]
    verify(bigQuery).startParameterizedQuery(any[GoogleProject], any[String], paramsCaptor.capture(), any[String])
    assertWorkflowParams(paramsCaptor, uncachedCosts.keySet)
  }

  it should "ask BigQuery for uncached workflows if some are cached" in withDefaultTestDatabase { _: SlickDataSource =>
    val submission = testData.costedSubmission1
    val submissionUuid = UUID.fromString(submission.submissionId)
    // 3 workflows, with 1 being cached
    val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
    val cachedCosts = costs.take(1)
    val uncachedCosts = costs -- cachedCosts.keySet
    // get the mocked service
    val (costService, bigQuery) = getMockitoSubmissionCostService(uncachedCosts)
    // persist cachedCosts
    Await.result(costService.writeCostsToLocalDb(submissionUuid, cachedCosts, Set()), Duration.Inf)
    // execute getSubmissionCosts
    val actual = Await.result(
      costService.getSubmissionCosts(
        testData.costedSubmission1.submissionId,
        costs.keySet.toSeq,
        defaultGoogleProjectId,
        DateTime.now().minusDays(7),
        Option(DateTime.now().minusDays(5))
      ),
      Duration.Inf
    )
    // verify correct results
    actual shouldBe costs
    // verify call to BigQuery
    val paramsCaptor = captor[List[QueryParameter]]
    verify(bigQuery).startParameterizedQuery(any[GoogleProject], any[String], paramsCaptor.capture(), any[String])
    assertWorkflowParams(paramsCaptor, uncachedCosts.keySet)
  }

  it should "bypass BigQuery if all workflows are cached" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val submission = testData.costedSubmission1
    val submissionUuid = UUID.fromString(submission.submissionId)
    // 3 workflows, all are cached
    val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
    val cachedCosts = costs
    val uncachedCosts = Map.empty[String, Float]
    // get the mocked service
    val (costService, bigQuery) = getMockitoSubmissionCostService(uncachedCosts)
    // persist cachedCosts
    Await.result(costService.writeCostsToLocalDb(submissionUuid, cachedCosts, Set()), Duration.Inf)
    // execute getSubmissionCosts
    val actual = Await.result(
      costService.getSubmissionCosts(
        testData.costedSubmission1.submissionId,
        costs.keySet.toSeq,
        defaultGoogleProjectId,
        DateTime.now().minusDays(7),
        Option(DateTime.now().minusDays(5))
      ),
      Duration.Inf
    )
    // verify correct results
    actual shouldBe costs
    // verify we didn't call BigQuery
    verify(bigQuery, never).startParameterizedQuery(any[GoogleProject],
                                                    any[String],
                                                    any[List[QueryParameter]],
                                                    any[String]
    )
  }

  it should "write BigQuery results back to cache" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    import dataSource.dataAccess.driver.api._

    val submission = testData.costedSubmission1
    val submissionUuid = UUID.fromString(submission.submissionId)
    // 3 workflows, with 1 being cached
    val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
    val cachedCosts = costs.take(1)
    val uncachedCosts = costs -- cachedCosts.keySet
    // get the mocked service
    val (costService, bigQuery) = getMockitoSubmissionCostService(uncachedCosts)
    // persist cachedCosts
    Await.result(costService.writeCostsToLocalDb(submissionUuid, cachedCosts, Set()), Duration.Inf)

    // validate what's in the cache before running getSubmissionCosts
    val actualCacheBefore = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
    actualCacheBefore.map(_.externalId) should contain theSameElementsAs cachedCosts.keySet

    // execute getSubmissionCosts
    Await.result(
      costService.getSubmissionCosts(
        testData.costedSubmission1.submissionId,
        costs.keySet.toSeq,
        defaultGoogleProjectId,
        DateTime.now().minusDays(7),
        Option(DateTime.now().minusDays(5))
      ),
      Duration.Inf
    )

    // validate what's in the cache after running getSubmissionCosts; ignore the WORKFLOW_ID column
    // because that is non-deterministic (it's an auto-increment column and is tested elsewhere)
    val actualCacheAfter = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
      .map(rec => rec.externalId -> rec.cost)
    val expectedCacheAfter = costs.map { case (externalId, cost) =>
      externalId -> Option(cost)
    }
    actualCacheAfter should contain theSameElementsAs expectedCacheAfter
  }

  it should "write nulls to cache when nothing cached and BigQuery has no results" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      import dataSource.dataAccess.driver.api._

      val submission = testData.costedSubmission1
      // 3 workflows; none cached, and none found in BigQuery
      val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
      val uncachedCosts = Map.empty[String, Float]
      // get the mocked service
      val (costService, _) = getMockitoSubmissionCostService(uncachedCosts)

      // validate what's in the cache before running getSubmissionCosts
      val cacheBefore = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
      cacheBefore shouldBe empty

      // execute getSubmissionCosts
      Await.result(
        costService.getSubmissionCosts(
          testData.costedSubmission1.submissionId,
          costs.keySet.toSeq,
          defaultGoogleProjectId,
          DateTime.now().minusDays(7),
          Option(DateTime.now().minusDays(5))
        ),
        Duration.Inf
      )

      // validate what's in the cache after running getSubmissionCosts; ignore the WORKFLOW_ID column
      // because that is non-deterministic (it's an auto-increment column and is tested elsewhere)
      val actualCacheAfter = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
        .map(rec => rec.externalId -> rec.cost)
      val expectedCacheAfter = costs.map { case (externalId, cost) =>
        externalId -> None
      }
      actualCacheAfter should contain theSameElementsAs expectedCacheAfter
  }

  it should "write nulls to cache when some cached and BigQuery has no results" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      import dataSource.dataAccess.driver.api._

      val submission = testData.costedSubmission1
      val submissionUuid = UUID.fromString(submission.submissionId)
      // 3 workflows, with 1 being cached, and none found in BigQuery
      val costs = submission.workflows.map(wf => wf.workflowId.get -> randomCost).toMap
      val cachedCosts = costs.take(1)
      val uncachedCosts = Map.empty[String, Float]
      // get the mocked service
      val (costService, bigQuery) = getMockitoSubmissionCostService(uncachedCosts)
      // persist cachedCosts
      Await.result(costService.writeCostsToLocalDb(submissionUuid, cachedCosts, Set()), Duration.Inf)

      // validate what's in the cache before running getSubmissionCosts
      val actualCacheBefore = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
        .map(_.externalId)
      actualCacheBefore should contain theSameElementsAs cachedCosts.keySet

      // execute getSubmissionCosts
      Await.result(
        costService.getSubmissionCosts(
          testData.costedSubmission1.submissionId,
          costs.keySet.toSeq,
          defaultGoogleProjectId,
          DateTime.now().minusDays(7),
          Option(DateTime.now().minusDays(5))
        ),
        Duration.Inf
      )

      // validate what's in the cache after running getSubmissionCosts; ignore the WORKFLOW_ID column
      // because that is non-deterministic (it's an auto-increment column and is tested elsewhere)
      val actualCacheAfter = runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)
        .map(rec => rec.externalId -> rec.cost)
      val nullRows = costs -- cachedCosts.keySet
      val expectedCacheAfter = cachedCosts.map { case (externalId, cost) =>
        externalId -> Option(cost)
      } ++ nullRows.map { case (externalId, cost) =>
        externalId -> None
      }
      actualCacheAfter should contain theSameElementsAs expectedCacheAfter
  }

  behavior of "writeCostsToLocalDb"

  it should "persist rows, including the foreign key to WORKFLOW" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      import dataSource.dataAccess.driver.api._

      val submission = testData.costedSubmission1
      val submissionUuid = UUID.fromString(submission.submissionId)
      // find existing workflows; assert all workflows have an external id
      submission.workflows.foreach { workflow =>
        workflow.workflowId should not be empty
      }
      // generate some random costs for these workflows
      val costs = submission.workflows.map { workflow =>
        workflow.workflowId.get -> // ensure floats have precision 2
          BigDecimal(Random.nextFloat() * 10)
            .setScale(2, RoundingMode.HALF_UP)
            .toFloat
      }.toMap
      // get the mocked service
      val (costService, _) = getMockitoSubmissionCostService(Map())
      // execute the writeCostsToLocalDb method
      val actualWrite = Await.result(
        costService.writeCostsToLocalDb(submissionUuid, costs, Set()),
        Duration.Inf
      )
      actualWrite shouldBe costs.size
      // retrieve the workflows, then retrieve the cached costs, and compare them
      val actualWorkflows =
        runAndWait(dataSource.dataAccess.workflowQuery.findWorkflowsBySubmissionId(submissionUuid).result)
          .map(rec => rec.id -> rec.externalId)
          .toMap
      val actualCostRecords =
        runAndWait(dataSource.dataAccess.workflowActualCostQuery.result)

      actualCostRecords.foreach { costRecord =>
        // the cost record's id should exist in the workflow table
        actualWorkflows.get(costRecord.id) should not be empty
        // the cost record's id->externalId pair should be the same as in the workflow table
        actualWorkflows(costRecord.id) shouldBe Option(costRecord.externalId)
        // the cost record's cost should be what we asked to insert
        costRecord.cost shouldBe Option(costs(costRecord.externalId))
      }

  }
}
