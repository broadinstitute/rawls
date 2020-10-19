package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.{ActorSystem, PoisonPill}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor.{ProcessNextWorkflow, ScheduleNextWorkflow, SubmitWorkflowBatch, WorkflowBatch}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.MockMarthaResolver._
import org.broadinstitute.dsde.rawls.mock.{MockMarthaResolver, MockSamDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockserver.model.HttpRequest
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by dvoet on 5/17/16.
 */
//noinspection TypeAnnotation,NameBooleanParameters,ScalaUnnecessaryParentheses,ScalaUnusedSymbol
class WorkflowSubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with BeforeAndAfterEach with RawlsTestUtils with Eventually with MockitoTestUtils with RawlsStatsDTestUtils {
  import driver.api._
  implicit val materializer = ActorMaterializer()

  def this() = this(ActorSystem("WorkflowSubmissionSpec"))
  val mockServer = RemoteServicesMockServer()
  val mockGoogleServicesDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  val mockSamDAO = new MockSamDAO(slickDataSource)
  val mockMarthaResolver = new MockMarthaResolver("https://martha_v3_url")
  private val requesterPaysRole = "requesterPays"

  /** Extension of WorkflowSubmission to allow us to intercept and validate calls to the execution service.
    */
  class TestWorkflowSubmission(
    val dataSource: SlickDataSource,
    val batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
    val processInterval: FiniteDuration = 250 milliseconds,
    val pollInterval: FiniteDuration = 1 second,
    val maxActiveWorkflowsTotal: Int = 100,
    val maxActiveWorkflowsPerUser: Int = 100,
    val runtimeOptions: Option[JsValue] = None,
    val trackDetailedSubmissionMetrics: Boolean = true,
    override val workbenchMetricBaseName: String = "test",
    val requesterPaysRole: String = requesterPaysRole,
    val useWorkflowCollectionField: Boolean = false,
    val useWorkflowCollectionLabel: Boolean = false,
    val methodConfigResolver: MethodConfigResolver = methodConfigResolver) extends WorkflowSubmission {

    val credential: Credential = mockGoogleServicesDAO.getPreparedMockGoogleCredential()

    val googleServicesDAO = mockGoogleServicesDAO
    val executionServiceCluster: ExecutionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), dataSource)
    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName)
    val samDAO = mockSamDAO
    val drsResolver = mockMarthaResolver
  }

  class TestWorkflowSubmissionWithMockExecSvc(
     dataSource: SlickDataSource,
     batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
     processInterval: FiniteDuration = 25 milliseconds,
     pollInterval: FiniteDuration = 1 second) extends TestWorkflowSubmission(dataSource, batchSize, pollInterval) {
    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), dataSource)
  }

  class TestWorkflowSubmissionWithTimeoutyExecSvc(
                                               dataSource: SlickDataSource,
                                               batchSize: Int = 3, // the mock remote server always returns 3, 2 success and an error
                                               processInterval: FiniteDuration = 25 milliseconds,
                                               pollInterval: FiniteDuration = 1 second) extends TestWorkflowSubmission(dataSource, batchSize, processInterval, pollInterval) {
    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(true), dataSource)
  }

  class TestWorkflowSubmissionWithLabels(dataSource: SlickDataSource,
                                         useWorkflowCollectionField: Boolean,
                                         useWorkflowCollectionLabel: Boolean) extends TestWorkflowSubmission(dataSource, 3, 25 milliseconds, 1 second, useWorkflowCollectionField = useWorkflowCollectionField, useWorkflowCollectionLabel = useWorkflowCollectionLabel)


  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result( mockGoogleServicesDAO.storeToken(userInfo, UUID.randomUUID.toString), Duration.Inf )
    mockServer.startServer()
  }

  override def beforeEach(): Unit = {
    mockGoogleServicesDAO.policies.clear()
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
    mockServer.stopServer
    super.afterAll()
  }

  "WorkflowSubmission" should "get a batch of workflows" in withDefaultTestDatabase {
    withStatsD {
      val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

      val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

      val workflowSubMsg = Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
      workflowSubMsg match {
        case SubmitWorkflowBatch(WorkflowBatch(actualWorkflowIds, submissionRec, workspaceRec)) =>
          assertSameElements(actualWorkflowIds, workflowRecs.map(_.id))
          submissionRec.id.toString should equal (testData.submission1.submissionId)
          workspaceRec.id.toString should equal (testData.workspace.workspaceId)
        case _ => fail("wrong workflow submission message")
      }

      assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Launching.toString))
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Launching))
    }
  }

  it should "not get a batch of workflows if beyond absolute cap" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, maxActiveWorkflowsTotal = 0)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    assertResult(ScheduleNextWorkflow) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Queued.toString))
  }

  it should "not get a batch of workflows if beyond user's cap" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, maxActiveWorkflowsPerUser = 0)

    val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    assertResult(ScheduleNextWorkflow) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }

    assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Queued.toString))
  }

  it should "get a batch of workflows if at user's cap" in withDefaultTestDatabase {
    withStatsD {
      val batchSize = 3
      val workflowSubmission = new TestWorkflowSubmission(slickDataSource, batchSize = batchSize, maxActiveWorkflowsPerUser = (runAndWait(workflowQuery.length.result) - batchSize))

      val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

      val unlaunchedBatch = Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
      unlaunchedBatch match {
        case SubmitWorkflowBatch(WorkflowBatch(workflowIds, submissionRec, workspaceRec)) =>
          assertSameElements(workflowIds, workflowRecs.map(_.id))
          submissionRec.id.toString should equal (testData.submission1.submissionId)
          workspaceRec.id.toString should equal (testData.workspace.workspaceId)
        case error => fail(s"Didn't schedule submission of a new workflow batch, instead got: $error")
      }

      assert(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).result).forall(_.status == WorkflowStatuses.Launching.toString))
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Launching))
    }
  }


  it should "schedule the next check when there are no workflows" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)
    assertResult(ScheduleNextWorkflow) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf)
    }
  }

  it should "have only 1 submission in a batch" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, 100)

    runAndWait(workflowQuery.batchUpdateStatus(WorkflowStatuses.Submitted, WorkflowStatuses.Queued))

    Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), Duration.Inf) match {
      case SubmitWorkflowBatch(WorkflowBatch(workflowIds, _, _)) =>
        assertResult(1)(runAndWait(workflowQuery.findWorkflowByIds(workflowIds).map(_.submissionId).distinct.result).size)
      case _ => fail("expected some workflows")
    }
  }

  it should "submit a batch of workflows" in withDefaultTestDatabase {
    withStatsD {
      val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

      withWorkspaceContext(testData.workspace) { ctx =>
        val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(testData.submission1, testData.workspace)

        assertResult(ProcessNextWorkflow) {
          Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)
        }

        assert({
          workflowRecs.forall(wf => (wf.status == WorkflowStatuses.Submitted.toString) || (wf.status == WorkflowStatuses.Failed.toString))
        }, "Not all workflows got submitted?")
      }
    } { capturedMetrics =>
      // should be 2 submitted, 1 failed per the mock Cromwell server
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Submitted, 2))
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Failed, 1))
    }
  }

  it should "submit a workflow with the right parameters and options" in withDefaultTestDatabase {
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource, 100, runtimeOptions = Some(JsObject(Map("zones" -> JsString("us-central-someother"))))) {
      override val executionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(testData.workspace) { ctx =>
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(testData.submission1, testData.workspace)

      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      assertResult(workflowRecs.map(_ => s"""{"${testData.inputResolutions.head.inputName}":"${testData.inputResolutions.head.value.get.asInstanceOf[AttributeString].value}"}""")) {
        mockExecCluster.getDefaultSubmitMember.asInstanceOf[MockExecutionServiceDAO].submitInput
      }

      val petJson = Await.result(workflowSubmission.samDAO.getPetServiceAccountKeyForUser(testData.workspace.namespace, testData.userOwner.userEmail), Duration.Inf)
      assertResult(
        Some(
          ExecutionServiceWorkflowOptions(
            jes_gcs_root = s"gs://${testData.workspace.bucketName}/${testData.submission1.submissionId}",
            google_project = testData.wsName.namespace,
            account_name = testData.userOwner.userEmail.value,
            google_compute_service_account = "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com",
            user_service_account_json =
            """{"client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com", "client_id": "104493171545941951815"}""",
            final_workflow_log_dir =
              s"gs://${testData.workspace.bucketName}/${testData.submission1.submissionId}/workflow.logs",
            default_runtime_attributes = Some(JsObject(Map("zones" -> JsString("us-central-someother")))),
            read_from_cache = false,
            delete_intermediate_output_files = false,
            google_labels = Map("terra-submission-id" -> s"terra-${submissionRec.id.toString}")
          ))) {
        mockExecCluster.getDefaultSubmitMember.asInstanceOf[MockExecutionServiceDAO].submitOptions.map(_.parseJson.convertTo[ExecutionServiceWorkflowOptions])
      }
    }
  }

  it should "submit a workflow that sets the workflow collection as neither a label nor a field in the request" in withDefaultTestDatabase {
    testWorkflowCollectionFieldAndLabels(false, false) { (mockExecService: MockExecutionServiceDAO, submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord) =>
      mockExecService.collection shouldEqual None
      mockExecService.labels shouldEqual Map("submission-id" -> submissionRec.id.toString, "workspace-id" -> workspaceRec.id.toString)
    }  }

  it should "submit a workflow that sets the workflow collection as a label" in withDefaultTestDatabase {
    testWorkflowCollectionFieldAndLabels(false, true) { (mockExecService: MockExecutionServiceDAO, submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord) =>
      mockExecService.collection shouldEqual None
      mockExecService.labels shouldEqual Map("submission-id" -> submissionRec.id.toString, "workspace-id" -> workspaceRec.id.toString, "caas-collection-name"  -> workspaceRec.workflowCollection.getOrElse(""))
    }
  }

  it should "submit a workflow that sets the workflow collection as a field in the request" in withDefaultTestDatabase {
    testWorkflowCollectionFieldAndLabels(true, false) { (mockExecService: MockExecutionServiceDAO, submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord) =>
      mockExecService.collection shouldEqual Some(workspaceRec.workflowCollection.getOrElse(""))
      mockExecService.labels shouldEqual  Map("submission-id" -> submissionRec.id.toString, "workspace-id" -> workspaceRec.id.toString)
    }
  }

  it should "submit a workflow that sets the workflow collection as both a label and a field in the request" in withDefaultTestDatabase {
    testWorkflowCollectionFieldAndLabels(true, true) { (mockExecService: MockExecutionServiceDAO, submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord) =>
      mockExecService.labels shouldEqual Map("submission-id" -> submissionRec.id.toString, "workspace-id" -> workspaceRec.id.toString, "caas-collection-name"  -> workspaceRec.workflowCollection.getOrElse(""))
      mockExecService.collection shouldEqual Some(workspaceRec.workflowCollection.getOrElse(""))
    }
  }

  def testWorkflowCollectionFieldAndLabels(useWorkflowCollectionField: Boolean, useWorkflowCollectionLabels: Boolean)(op: (MockExecutionServiceDAO, SubmissionRecord, WorkspaceRecord) => _) = {
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmissionWithLabels(slickDataSource, useWorkflowCollectionField, useWorkflowCollectionLabels) {
      override val executionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(testData.workspace) { ctx =>
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(testData.submission1, testData.workspace)
      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      val mockExecService = mockExecCluster.getDefaultSubmitMember.asInstanceOf[MockExecutionServiceDAO]
      op(mockExecService, submissionRec, workspaceRec)
    }
  }

  it should "resolve DOS URIs when submitting workflows" in withDefaultTestDatabase {
    val data = testData
    // Set up system under test
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource) {
      override val executionServiceCluster: ExecutionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(data.workspace) { ctx =>
      // Create test submission data
      val sample = Entity("sample", "Sample", Map())
      val sampleSet = Entity("sampleset", "sample_set",
        Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample.toReference))))
      val inputResolutions = Seq(
        SubmissionValidationValue(Option(AttributeString("dos://foo/bar")), None, "test_input_dos"),
        SubmissionValidationValue(Option(AttributeValueList(Seq(AttributeString("dos://foo/bar1"), AttributeString("dos://different"), AttributeString("dos://foo/bar3")))), None, "test_input_dos_array"))
      val submissionDos = createTestSubmission(data.workspace, data.agoraMethodConfig, sampleSet, WorkbenchEmail(data.userOwner.userEmail.value),
        Seq(sample), Map(sample -> inputResolutions), Seq(), Map())

      runAndWait(entityQuery.save(ctx, sample))
      runAndWait(entityQuery.save(ctx, sampleSet))
      runAndWait(submissionQuery.create(ctx, submissionDos))
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(submissionDos, data.workspace)

      // Submit workflow!
      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      // Verify
      mockGoogleServicesDAO.policies(ctx.googleProject)(requesterPaysRole) should contain theSameElementsAs
        List("serviceAccount:" + drsServiceAccount, "serviceAccount:" + differentDrsServiceAccount)
    }
  }

  it should "resolve DRS URIs when submitting workflows" in withDefaultTestDatabase {
    val data = testData
    // Set up system under test
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource) {
      override val executionServiceCluster: ExecutionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(data.workspace) { ctx =>
      // Create test submission data
      val sample = Entity("sample", "Sample", Map())
      val sampleSet = Entity("sampleset", "sample_set",
        Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample.toReference))))
      val inputResolutions = Seq(
        SubmissionValidationValue(Option(AttributeString("drs://foo/bar")), None, "test_input_dos"),
        SubmissionValidationValue(Option(AttributeValueList(Seq(AttributeString("drs://foo/bar1"), AttributeString("drs://different"), AttributeString("drs://foo/bar3")))), None, "test_input_dos_array"))
      val submissionDrs = createTestSubmission(data.workspace, data.agoraMethodConfig, sampleSet, WorkbenchEmail(data.userOwner.userEmail.value),
        Seq(sample), Map(sample -> inputResolutions), Seq(), Map())

      runAndWait(entityQuery.save(ctx, sample))
      runAndWait(entityQuery.save(ctx, sampleSet))
      runAndWait(submissionQuery.create(ctx, submissionDrs))
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(submissionDrs, data.workspace)

      // Submit workflow!
      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      // Verify
      mockGoogleServicesDAO.policies(ctx.googleProject)(requesterPaysRole) should contain theSameElementsAs
        List("serviceAccount:" + drsServiceAccount, "serviceAccount:" + differentDrsServiceAccount)
    }
  }

  it should "resolve DRS URIs containing Jade Data Repo urls when submitting workflows" in withDefaultTestDatabase {
    val data = testData
    // Set up system under test
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource) {
      override val executionServiceCluster: ExecutionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(data.workspace) { ctx =>
      // Create test submission data
      val sample = Entity("sample", "Sample", Map())
      val sampleSet = Entity("sampleset", "sample_set",
        Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample.toReference))))
      val inputResolutions = Seq(
        SubmissionValidationValue(Option(AttributeString("drs://foo/bar")), None, "test_input_dos"),
        SubmissionValidationValue(
          Option(AttributeValueList(Seq(
            AttributeString("drs://jade.datarepo-dev.broadinstitute.org/v1_abc-123"),
            AttributeString("drs://different"),
            AttributeString("drs://foo/bar3")))
          ),
          None,
          "test_input_dos_array"
        ))
      val submissionDrs = createTestSubmission(data.workspace, data.agoraMethodConfig, sampleSet, WorkbenchEmail(data.userOwner.userEmail.value),
        Seq(sample), Map(sample -> inputResolutions), Seq(), Map())

      runAndWait(entityQuery.save(ctx, sample))
      runAndWait(entityQuery.save(ctx, sampleSet))
      runAndWait(submissionQuery.create(ctx, submissionDrs))
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(submissionDrs, data.workspace)

      // Submit workflow!
      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      // Verify
      val expectedClientEmail = List("serviceAccount:" + drsServiceAccount, "serviceAccount:" + differentDrsServiceAccount)
      mockGoogleServicesDAO.policies(ctx.googleProject)(requesterPaysRole) should contain theSameElementsAs expectedClientEmail
    }
  }

  it should "resolve DRS URIs containing only Jade Data Repo urls when submitting workflows" in withDefaultTestDatabase {
    val data = testData
    // Set up system under test
    val mockExecCluster = MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(), slickDataSource)
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource) {
      override val executionServiceCluster: ExecutionServiceCluster = mockExecCluster
    }

    withWorkspaceContext(data.workspace) { ctx =>
      // Create test submission data
      val sample = Entity("sample", "Sample", Map())
      val sampleSet = Entity("sampleset", "sample_set",
        Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(sample.toReference))))
      val inputResolutions = Seq(
        SubmissionValidationValue(Option(AttributeString("drs://jade.datarepo-dev.broadinstitute.org/v1_abc-123")), None, "test_input_dos"),
        SubmissionValidationValue(
          Option(AttributeValueList(Seq(
            AttributeString("drs://jade.datarepo-dev.broadinstitute.org/v1_abc-345"),
            AttributeString("drs://jade.datarepo-dev.broadinstitute.org/v1_abc-678"),
            AttributeString("drs://jade.datarepo-dev.broadinstitute.org/v1_def-123")))
          ),
          None,
          "test_input_dos_array"
        ))
      val submissionDrs = createTestSubmission(data.workspace, data.agoraMethodConfig, sampleSet, WorkbenchEmail(data.userOwner.userEmail.value),
        Seq(sample), Map(sample -> inputResolutions), Seq(), Map())

      runAndWait(entityQuery.save(ctx, sample))
      runAndWait(entityQuery.save(ctx, sampleSet))
      runAndWait(submissionQuery.create(ctx, submissionDrs))
      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(submissionDrs, data.workspace)

      // Submit workflow!
      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      // Verify
      // since no SA was returned from Martha for JDR urls, requester pays role was never added to the polices. Hence it should be empty
      mockGoogleServicesDAO.policies shouldBe TrieMap.empty[RawlsBillingProjectName, Map[String, Set[String]]]
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
      val sset = Entity("testset6", "SampleSet", Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(samples)))

      runAndWait(entityQuery.save(testData.workspace, sset))

      def inputResolutions(sampleName: String) = {
        Seq(SubmissionValidationValue(Option(AttributeString(sampleName)), Option("message"), "three_step.cgrep.pattern"))
      }

      val thisSubmission = createTestSubmission(testData.workspace, testData.methodConfigValid, sset, WorkbenchEmail(testData.userOwner.userEmail.value),
        Seq(testData.sample1, testData.sample2, testData.sample3, testData.sample4, testData.sample5, testData.sample6),
        Map(testData.sample1 -> inputResolutions("sample1"), testData.sample2 -> inputResolutions("sample2"),
          testData.sample3 -> inputResolutions("sample3"), testData.sample4 -> inputResolutions("sample4"),
          testData.sample5 -> inputResolutions("sample5"), testData.sample6 -> inputResolutions("sample6")), Seq(), Map(), WorkflowStatuses.Queued
      )
      runAndWait(submissionQuery.create(ctx, thisSubmission))

      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(thisSubmission, testData.workspace)

      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      assertResult(samples.map(r => r.entityName -> r.entityName).toMap) {
        runAndWait(submissionQuery.loadSubmission(UUID.fromString(thisSubmission.submissionId))).get.workflows.map { wf =>
          wf.workflowEntity.get.entityName -> wf.workflowId.get
        }.toMap
      }
    }
  }

  it should "fail workflows that timeout when submitting to Cromwell" in withDefaultTestDatabase {
    withStatsD {
      val workflowSubmission = new TestWorkflowSubmissionWithTimeoutyExecSvc(slickDataSource)

      withWorkspaceContext(testData.workspace) { ctx =>

        val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(testData.submission1, testData.workspace)

        //This call throws an exception, which is piped back to the actor
        val submitFailureExc = intercept[RawlsExceptionWithErrorReport] {
          Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)
        }
        assert(submitFailureExc.errorReport.statusCode.contains(StatusCodes.GatewayTimeout))

        //Should have marked the workflows as failed, though
        val newWorkflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))
        assert({
          newWorkflowRecs.forall(_.status == WorkflowStatuses.Failed.toString)
        }, "Workflows that timeout on submission to Cromwell should be marked Failed")
      }
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Failed))
    }
  }

  "WorkflowSubmissionActor" should "submit workflows" in withDefaultTestDatabase {
    withStatsD {
      val credential: Credential = mockGoogleServicesDAO.getPreparedMockGoogleCredential()

      val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(3, testData.submission1.submissionId)

      val workflowSubmissionActor = system.actorOf(WorkflowSubmissionActor.props(
        slickDataSource,
        new HttpMethodRepoDAO(
          MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
          MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
          workbenchMetricBaseName = workbenchMetricBaseName),
        mockGoogleServicesDAO,
        mockSamDAO,
        mockMarthaResolver,
        MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource),
        3, credential, 1 milliseconds, 1 milliseconds, 100, 100, None, true, "test", requesterPaysRole, false, false,
        methodConfigResolver)
      )

      awaitCond(
        runAndWait(
          workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result
        ).contains(WorkflowStatuses.Submitted.toString),
        10.seconds
      )
      workflowSubmissionActor ! PoisonPill
    } { capturedMetrics =>
      // should be 2 submitted, 1 failed per the mock Cromwell server
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Submitted, 2))
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submission1, WorkflowStatuses.Failed, 1))

      val expected = expectedHttpRequestMetrics("post", "api.workflows.v1.batch", StatusCodes.Created.intValue, 1, Some(Subsystems.Cromwell))
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "continue submitting workflow on exec svc error" in withDefaultTestDatabase {
    withStatsD {
      val credential: Credential = mockGoogleServicesDAO.getPreparedMockGoogleCredential()

      val batchSize = 3
      val workflowRecs: Seq[WorkflowRecord] = setWorkflowBatchToQueued(2 * batchSize, testData.submissionTerminateTest.submissionId)

      // we want to test that the actor makes more than one pass so assert that there will be more than one batch
      assert(workflowRecs.size > batchSize)

      val workflowSubmissionActor = system.actorOf(WorkflowSubmissionActor.props(
        slickDataSource,
        new HttpMethodRepoDAO(
          MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
          MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
          workbenchMetricBaseName = workbenchMetricBaseName),
        mockGoogleServicesDAO,
        mockSamDAO,
        mockMarthaResolver,
        MockShardedExecutionServiceCluster.fromDAO(new MockExecutionServiceDAO(true), slickDataSource),
        batchSize, credential, 1 milliseconds, 1 milliseconds, 100, 100, None, true, "test", requesterPaysRole, false, false,
        methodConfigResolver)
      )

      awaitCond(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result).forall(_ == WorkflowStatuses.Failed.toString), 10 seconds)
      workflowSubmissionActor ! PoisonPill
    } { capturedMetrics =>
      capturedMetrics should contain (expectedWorkflowStatusMetric(testData.workspace, testData.submissionTerminateTest, WorkflowStatuses.Failed))
    }
  }

  it should "not truncate array inputs" in withDefaultTestDatabase {

    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    withWorkspaceContext(testData.workspace) { ctx =>

      val inputResolutionsList = Seq(SubmissionValidationValue(Option(
        AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))), Option("message3"), "test_input_name3"))

      val submissionList = createTestSubmission(testData.workspace, testData.methodConfigArrayType, testData.sset1, WorkbenchEmail(testData.userOwner.userEmail.value),
        Seq(testData.sset1), Map(testData.sset1 -> inputResolutionsList),
        Seq.empty, Map.empty)

      runAndWait(submissionQuery.create(ctx, submissionList))

      val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(submissionList, testData.workspace)

      Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), Duration.Inf)

      val arrayWdl = """task aggregate_data {
                       |	Array[String] input_array
                       |
                       |	command {
                       |    echo "foo"
                       |
                       |	}
                       |
                       |	output {
                       |		Array[String] output_array = input_array
                       |	}
                       |
                       |	runtime {
                       |		docker : "broadinstitute/aaaa:31"
                       |	}
                       |
                       |	meta {
                       |		author : "Barack Obama"
                       |		email : "barryo@whitehouse.gov"
                       |	}
                       |
                       |}
                       |
                       |workflow aggregate_data_workflow {
                       |	call aggregate_data
                       |}""".stripMargin

      val inputs = Map("test_input_name3" -> List("elem1", "elem2", "elem3")).toJson.toString

      // confirm that we have sent to MockServer a request containing the WDL and full set of inputs

      mockServer.mockServer.verify(
        HttpRequest.request()
          .withMethod("POST")
          .withPath("/api/workflows/v1/batch")
          .withBody(mockServerContains(arrayWdl))
          .withBody(mockServerContains(inputs)),
        VerificationTimes.once()
      )

    }
  }

  it should "pass workflow failure modes to cromwell" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)
    val (workflowRecs, submissionRec, workspaceRec) = getWorkflowSubmissionWorkspaceRecords(testData.submissionWorkflowFailureMode, testData.workspaceWorkflowFailureMode)
    Await.result(workflowSubmission.submitWorkflowBatch(WorkflowBatch(workflowRecs.map(_.id), submissionRec, workspaceRec)), 10 seconds)

    mockServer.mockServer.verify(
      HttpRequest.request()
        .withMethod("POST")
        .withPath("/api/workflows/v1/batch")
        .withBody(mockServerContains("workflow_failure_mode"))
        .withBody(mockServerContains("ContinueWhilePossible")),
      VerificationTimes.once()
    )
  }

  it should "not submit workflows whose submission is aborting" in withDefaultTestDatabase {
    val workflowSubmission = new TestWorkflowSubmission(slickDataSource)

    // Set workflows to Queued
    setWorkflowBatchToQueued(workflowSubmission.batchSize, testData.submission1.submissionId)

    // Set submission to Aborting
    runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Aborting))

    // The queued workflows should not be launchable
    assertResult(ScheduleNextWorkflow) {
      Await.result(workflowSubmission.getUnlaunchedWorkflowBatch(), 1 minute)
    }
  }

  private def setWorkflowBatchToQueued(batchSize: Int, submissionId: String): Seq[WorkflowRecord] = {
    runAndWait(
      for {
        workflowRecs <- workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(submissionId)).take(batchSize).result
        _ <- workflowQuery.batchUpdateStatus(workflowRecs, WorkflowStatuses.Queued)
      } yield {
        workflowRecs
      }
    )
  }

  private def getWorkflowSubmissionWorkspaceRecords(submission: Submission, workspace: Workspace): (Seq[WorkflowRecord], SubmissionRecord, WorkspaceRecord) = {
    runAndWait(
      for {
        wfRecs <- workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(submission.submissionId))
        subRec <- submissionQuery.findById(UUID.fromString(submission.submissionId)).result.map(_.head)
        wsRec <- workspaceQuery.findByIdQuery(UUID.fromString(workspace.workspaceId)).result.map(_.head)
      } yield (wfRecs, subRec, wsRec)
    )
  }
}
