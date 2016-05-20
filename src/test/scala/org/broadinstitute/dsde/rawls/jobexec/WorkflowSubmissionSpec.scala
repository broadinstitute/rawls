package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.{PoisonPill, ActorSystem}
import akka.testkit.TestKit
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkflowRecord, TestDriverComponent, TestDriverComponentWithFlatSpecAndMatchers}
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor.{ScheduleNextWorkflowQuery, SubmitWorkflowBatch}
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import spray.http.StatusCodes

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

/**
 * Created by dvoet on 5/17/16.
 */
class WorkflowSubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll {
  import driver.api._

  def this() = this(ActorSystem("WorkflowSubmissionSpec"))
  val mockGoogleServicesDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  val mockServer = RemoteServicesMockServer()

  class TestWorkflowSubmission(
    val dataSource: SlickDataSource,
    val batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
    val pollInterval: FiniteDuration = 1 second,
    val maxActiveWorkflowsTotal: Int = 100,
    val maxActiveWorkflowsPerUser: Int = 100) extends WorkflowSubmission {

    val credential: Credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)

    val googleServicesDAO = mockGoogleServicesDAO
    val executionServiceDAO: ExecutionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout)
    val methodRepoDAO = new HttpMethodRepoDAO(mockServer.mockServerBaseUrl)
  }

  class TestWorkflowSubmissionWithMockExecSvc(
     dataSource: SlickDataSource,
     batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
     pollInterval: FiniteDuration = 1 second) extends TestWorkflowSubmission(dataSource, batchSize, pollInterval) {
    override val executionServiceDAO = new MockExecutionServiceDAO()
  }

  class TestWorkflowSubmissionWithTimeoutyExecSvc(
                                               dataSource: SlickDataSource,
                                               batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
                                               pollInterval: FiniteDuration = 1 second) extends TestWorkflowSubmission(dataSource, batchSize, pollInterval) {
    override val executionServiceDAO = new MockExecutionServiceDAO(true)
  }

  override def beforeAll() = {
    super.beforeAll
    Await.result( mockGoogleServicesDAO.storeToken(userInfo, UUID.randomUUID.toString), Duration.Inf )
    mockServer.startServer
  }

  override def afterAll() = {
    system.shutdown()
  }

  "WorkflowSubmission" should "get a batch of workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    assertResult(SubmitWorkflowBatch(workflowRecs.map(_.id))) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Launching.toString))
  }

  def setWorkflowBatchToQueued(batchSize: Int, submissionId: String): Seq[WorkflowRecord] = {
    val workflowRecs = runAndWait(
      for {
        workflowRecs <- workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(submissionId)).take(batchSize).result
        _ <- workflowQuery.batchUpdateStatus(workflowRecs, WorkflowStatuses.Queued)
      } yield {
        workflowRecs
      }
    )
    workflowRecs
  }

  it should "not get a batch of workflows if beyond absolute cap" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, maxActiveWorkflowsTotal = 0)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    assertResult(ScheduleNextWorkflowQuery) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Queued.toString))
  }

  it should "not get a batch of workflows if beyond user's cap" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, maxActiveWorkflowsPerUser = 0)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    assertResult(ScheduleNextWorkflowQuery) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Queued.toString))
  }

  it should "get a batch of workflows if at user's cap" in withDefaultTestDatabase {
    val batchSize = 3
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, batchSize = batchSize, maxActiveWorkflowsPerUser = (runAndWait(workflowQuery.length.result) - batchSize))

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    val unlaunchedBatch = Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    unlaunchedBatch match {
      case SubmitWorkflowBatch(ids) =>
        assert(ids.toSet == workflowRecs.map(_.id).toSet) //order isn't consistent, apparently
      case error => fail(s"Didn't schedule submission of a new workflow batch, instead got: $error")
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Launching.toString))
  }


  it should "schedule the next check when there are no workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)
    assertResult(ScheduleNextWorkflowQuery) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }
  }

  it should "have only 1 submission in a batch" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, 100)

    runAndWait(workflowQuery.batchUpdateStatus(WorkflowStatuses.Submitted, WorkflowStatuses.Queued))

    Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf) match {
      case SubmitWorkflowBatch(workflowIds) =>
        assertResult(1)(runAndWait(workflowQuery.findWorkflowByIds(workflowIds).map(_.submissionId).distinct.result).size)
      case _ => fail("expected some workflows")
    }
  }

  it should "submit a batch of workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    withWorkspaceContext(testData.workspace) { ctx =>

      val workflowIds = runAndWait(workflowQuery.getWithWorkflowIds(ctx, testData.submission1.submissionId)).map(_._1)

      assertResult(ScheduleNextWorkflowQuery) {
        Await.result(workflowSubmission.submitWorkflowBatch(workflowIds), Duration.Inf)
      }

      val workflowRecs = runAndWait(workflowQuery.filter(_.id inSetBind workflowIds).result)
      assert({
        workflowRecs.forall( wf => (wf.status == WorkflowStatuses.Submitted.toString) || (wf.status == WorkflowStatuses.Failed.toString) )
      }, "Not all workflows got submitted?")
    }
  }

  it should "submit a workflow with the right parameters and options" in withDefaultTestDatabase {
    val mockExecSvc = new MockExecutionServiceDAO()
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, 100) {
      override val executionServiceDAO = mockExecSvc
    }

    withWorkspaceContext(testData.workspace) { ctx =>

      val workflowIds = runAndWait(workflowQuery.getWithWorkflowIds(ctx, testData.submission1.submissionId)).map(_._1)
      Await.result(workflowSubmission.submitWorkflowBatch(workflowIds), Duration.Inf)

      assertResult(workflowIds.map(_ => s"""{"${testData.inputResolutions.head.inputName}":"${testData.inputResolutions.head.value.get.asInstanceOf[AttributeString].value}"}""")) {
        mockExecSvc.submitInput
      }

      import spray.json._
      import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat // implicit format make convertTo work below
      val token = Await.result(workflowSubmission.googleServicesDAO.getToken(testData.submission1.submitter), Duration.Inf).get
      assertResult(Some(ExecutionServiceWorkflowOptions(s"gs://${testData.workspace.bucketName}/${testData.submission1.submissionId}", testData.wsName.namespace, testData.userOwner.userEmail.value, token, testData.billingProject.cromwellAuthBucketUrl))) {
        mockExecSvc.submitOptions.map(_.parseJson.convertTo[ExecutionServiceWorkflowOptions])
      }
    }
  }

  it should "match workflows to entities they run on in the right order" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmissionWithMockExecSvc(slickDataSource)

    withWorkspaceContext(testData.workspace) { ctx =>

      val samples = Seq(
        AttributeEntityReference("Sample", "sample1"),
        AttributeEntityReference("Sample", "sample2"),
        AttributeEntityReference("Sample", "sample3"),
        AttributeEntityReference("Sample", "sample4"),
        AttributeEntityReference("Sample", "sample5"),
        AttributeEntityReference("Sample", "sample6"))
      val sset = Entity("testset6", "SampleSet", Map( "samples" -> AttributeEntityReferenceList(samples)))

      runAndWait(entityQuery.save(SlickWorkspaceContext(testData.workspace), sset))

      def inputResolutions(sampleName: String) = {
        Seq(SubmissionValidationValue(Option(AttributeString(sampleName)), Option("message"), "three_step.cgrep.pattern"))
      }

      val thisSubmission = createTestSubmission(testData.workspace, testData.methodConfigValid, sset, testData.userOwner,
        Seq(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6),
        Map(testData.sample1 -> inputResolutions("sample1"), testData.sample2 -> inputResolutions("sample2"),
          testData.sample3 -> inputResolutions("sample3"), testData.sample4 -> inputResolutions("sample4"),
          testData.sample5 -> inputResolutions("sample5"), testData.sample6 -> inputResolutions("sample6")), Seq(), Map(), WorkflowStatuses.Queued
      )
      runAndWait(submissionQuery.create(ctx, thisSubmission))

      val wfIds = runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(thisSubmission.submissionId)).result.map( _.map(_.id) ))
      Await.result(workflowSubmission.submitWorkflowBatch(wfIds), Duration.Inf)

      assertResult(samples.map(r => r.entityName -> r.entityName).toMap) {
        runAndWait(submissionQuery.loadSubmission(UUID.fromString(thisSubmission.submissionId))).get.workflows.map { wf =>
          wf.workflowEntity.get.entityName -> wf.workflowId.get
        }.toMap
      }
    }
  }

  it should "fail workflows that timeout when submitting to Cromwell" in withDefaultTestDatabase {

    val workflowSubmission = new TestWorkflowSubmissionWithTimeoutyExecSvc(slickDataSource)

    withWorkspaceContext(testData.workspace) { ctx =>

      val workflowIds = runAndWait(workflowQuery.getWithWorkflowIds(ctx, testData.submission1.submissionId)).map(_._1)

      //This call throws an exception, which is piped back to the actor
      val submitFailureExc = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workflowSubmission.submitWorkflowBatch(workflowIds), Duration.Inf)
      }
      assert(submitFailureExc.errorReport.statusCode.contains(StatusCodes.GatewayTimeout))

      //Should have marked the workflows as failed, though
      val workflowRecs = runAndWait(workflowQuery.filter(_.id inSetBind workflowIds).result)
      assert({
        workflowRecs.forall( _.status == WorkflowStatuses.Failed.toString )
      }, "Workflows that timeout on submission to Cromwell should be marked Failed")
    }
  }

  "WorkflowSubmissionActor" should "submit workflows" in withDefaultTestDatabase {
    val credential: Credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(3, testData.submission1.submissionId)

    val workflowSubmissionActor = system.actorOf(WorkflowSubmissionActor.props(
      slickDataSource,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      mockGoogleServicesDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout),
      3, credential, 1 milliseconds, 100, 100)
    )

    awaitCond(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result).forall(_ != WorkflowStatuses.Queued.toString), 10 seconds)
    workflowSubmissionActor ! PoisonPill
  }

  it should "continue submitting workflow on exec svc error" in withDefaultTestDatabase {
    val credential: Credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)

    val batchSize = 3
    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(2*batchSize, testData.submissionTerminateTest.submissionId)

    // we want to test that the actor makes more than one pass so assert that there will be more than one batch
    assert(workflowRecs.size > batchSize)

    val workflowSubmissionActor = system.actorOf(WorkflowSubmissionActor.props(
      slickDataSource,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      mockGoogleServicesDAO,
      new MockExecutionServiceDAO(true),
      batchSize, credential, 1 milliseconds, 100, 100)
    )

    awaitCond(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result).forall(_ == WorkflowStatuses.Failed.toString), 10 seconds)
    workflowSubmissionActor ! PoisonPill
  }
}
