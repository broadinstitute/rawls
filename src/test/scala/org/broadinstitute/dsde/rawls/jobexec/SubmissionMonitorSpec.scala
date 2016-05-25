package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.testkit.{TestActorRef, TestKit}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential.Builder
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor.{StatusCheckComplete, ExecutionServiceStatusResponse}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{Matchers, FlatSpecLike}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.Success
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkflowRecord, TestDriverComponent}
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by dvoet on 7/1/15.
 */
class SubmissionMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll {
  import driver.api._

  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val testDbName = "SubmissionMonitorSpec"

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  private def await[T](f: Future[T]): T = Await.result(f, 5 minutes)

  "SubmissionMonitor" should "queryExecutionServiceForStatus success" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Success(Option((workflowRec.copy(status = WorkflowStatuses.Succeeded.toString), Some(ExecutionServiceOutputs(workflowRec.externalId.get, Map("o1" -> AttributeString("foo"))))))) }
    ))) {
      ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
    }
  }

  it should "queryExecutionServiceForStatus submitted" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Submitted.toString))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Success(None) }
    ))) {
      ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
    }
  }

  (WorkflowStatuses.runningStatuses.toSet ++ WorkflowStatuses.terminalStatuses -- Set(WorkflowStatuses.Succeeded, WorkflowStatuses.Submitted)).foreach { status =>
    it should s"queryExecutionServiceForStatus $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(status.toString))

      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

      assertResult(ignoreStatusLastChangedDate(ExecutionServiceStatusResponse(
        workflowsRecs.map { workflowRec => scala.util.Success(Option((workflowRec.copy(status = status.toString), None))) }
      ))) {
        ignoreStatusLastChangedDate(await(monitor.queryExecutionServiceForStatus()))
      }
    }
  }

  val abortableStatuses = Seq(WorkflowStatuses.Queued) ++ WorkflowStatuses.runningStatuses

  abortableStatuses.foreach { status =>
    it should s"abort all ${status.toString} workflows for a submission marked as aborting" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

      runAndWait(workflowQuery.batchUpdateStatus(workflowRecs, status))
      runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Aborting))

      val submission = runAndWait(submissionQuery.loadSubmission(UUID.fromString(testData.submission1.submissionId))).get
      assert(submission.status == SubmissionStatuses.Aborting)

      assertResult(Seq.fill(workflowRecs.size){status.toString}) {
        runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result)
      }

      val monitorRef = createSubmissionMonitorActor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Submitted.toString))

      awaitCond(runAndWait(workflowQuery.findWorkflowByIds(workflowRecs.map(_.id)).map(_.status).result).forall(_ == WorkflowStatuses.Aborted.toString), 10 seconds)
    }
  }

  it should "queryExecutionServiceForStatus exception" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val exception = new RuntimeException
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(throw exception))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assertResult(ExecutionServiceStatusResponse(
      workflowsRecs.map { workflowRec => scala.util.Failure(exception) }
    )) {
      await(monitor.queryExecutionServiceForStatus())
    }
  }

  WorkflowStatuses.queuedStatuses.foreach { status =>
    it should s"checkOverallStatus queued status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

      val initialStatus = SubmissionStatuses.Submitted
      runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

      assert(!runAndWait(monitor.checkOverallStatus(this)), "Queued workflows should not result in the submission changing state")

      assertResult(initialStatus.toString) {
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
      }
    }
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"checkOverallStatus running status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

      Set(SubmissionStatuses.Aborting, SubmissionStatuses.Submitted).foreach { initialStatus =>
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

        assert(!runAndWait(monitor.checkOverallStatus(this)))

        assertResult(initialStatus.toString) {
          runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
        }
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"checkOverallStatus terminal status - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

      runAndWait(workflowQuery.findWorkflowsBySubmissionId(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(status.toString))

      Set(SubmissionStatuses.Aborting, SubmissionStatuses.Submitted).foreach { initialStatus =>
        runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).map(_.status).update(initialStatus.toString))

        assert(runAndWait(monitor.checkOverallStatus(this)))

        assertResult(if (initialStatus == SubmissionStatuses.Aborting) SubmissionStatuses.Aborted.toString else SubmissionStatuses.Done.toString) {
          runAndWait(submissionQuery.findById(UUID.fromString(testData.submission1.submissionId)).result.head).status
        }
      }
    }
  }

  it should "attachOutputs normal" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0), ExecutionServiceOutputs("foo", Map("output" -> AttributeString("hello world!"), "output2" -> AttributeString("hello world."), "output3" -> AttributeString("hello workspace."), "extra" -> AttributeString("hello world!")))))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExprepressions: Map[String, String] = Map("output" -> "this.bar", "output2" -> "this.baz", "output3" -> "workspace.garble")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left(
      (Option(entity.copy(attributes = entity.attributes ++ Map("bar" -> AttributeString("hello world!"), "baz" -> AttributeString("hello world.")))),
        Option(testData.workspace.copy(attributes = testData.workspace.attributes + ("garble" -> AttributeString("hello workspace.")))))))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExprepressions)
    }
  }

  it should "attachOutputs only entities" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0), ExecutionServiceOutputs("foo", Map("output" -> AttributeString("hello world!"), "output2" -> AttributeString("hello world."), "output3" -> AttributeString("hello workspace."), "extra" -> AttributeString("hello world!")))))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExprepressions: Map[String, String] = Map("output" -> "this.bar", "output2" -> "this.baz")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left(
      (Option(entity.copy(attributes = entity.attributes ++ Map("bar" -> AttributeString("hello world!"), "baz" -> AttributeString("hello world.")))),
        None)))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExprepressions)
    }
  }

  it should "attachOutputs none" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0), ExecutionServiceOutputs("foo", Map("output" -> AttributeString("hello world!"), "output2" -> AttributeString("hello world."), "output3" -> AttributeString("hello workspace."), "extra" -> AttributeString("hello world!")))))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExprepressions: Map[String, String] = Map.empty

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Left((None, None)))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExprepressions)
    }
  }

  it should "attachOutputs missing expected output" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val entityId = 0.toLong
    val entity = Entity("e", "t", Map.empty)
    val workflowRecord = WorkflowRecord(1, Option("foo"), UUID.randomUUID(), WorkflowStatuses.Succeeded.toString, null, entityId, 0)
    val workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)] = Seq((workflowRecord, ExecutionServiceOutputs("foo", Map("output" -> AttributeString("hello world!")))))
    val entitiesById: Map[Long, Entity] = Map(entityId -> entity)
    val outputExprepressions: Map[String, String] = Map("missing" -> "this.bar")

    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    assertResult(Seq(Right((workflowRecord, Seq(AttributeString(s"output named missing does not exist")))))) {
      monitor.attachOutputs(testData.workspace, workflowsWithOutputs, entitiesById, outputExprepressions)
    }
  }

  it should "save workflow error messages" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submission1, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))

    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    runAndWait(monitor.saveErrors(workflowsRecs.map(r => (r, Seq(AttributeString("a"), AttributeString("b")))), this))

    val submission = runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submission1.submissionId)).get

    assert(submission.workflows.forall(_.status == WorkflowStatuses.Failed))

    submission.workflows.foreach { workflow =>
      assertResult(Seq(AttributeString("a"), AttributeString("b"))) {
        workflow.messages
      }
    }
  }

  it should "handle outputs" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Succeeded.toString))
    val workflowRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

    runAndWait(monitor.handleOutputs(workflowRecs.map(r => (r, ExecutionServiceOutputs(r.externalId.get, Map("o1" -> AttributeString("result"))))), this))

    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes + ("foo" -> AttributeString("result"))))) {
      testData.submissionUpdateEntity.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }
  }

  it should "handleStatusResponses with no workflows" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val monitor = createSubmissionMonitor(dataSource, testData.submissionNoWorkflows, new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Running.toString))

    assertResult(StatusCheckComplete(true)) {
      await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(Seq.empty)))
    }
  }

  WorkflowStatuses.queuedStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - queued - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)) flatMap { workflowRecs =>
        workflowQuery.batchUpdateStatus(workflowRecs, status)
      })
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))

      assertResult(StatusCheckComplete(false)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(Seq.empty)))
      }
    }
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - running - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))
      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

      assertResult(StatusCheckComplete(false)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), None)))))))
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"handleStatusResponses from exec svc - terminal - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))
      val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

      assertResult(StatusCheckComplete(true)) {
        await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), None)))))))
      }
    }
  }

  it should s"handleStatusResponses from exec svc - success with outputs" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    val status = WorkflowStatuses.Succeeded
    val monitor = createSubmissionMonitor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))
    val workflowsRecs = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submissionUpdateEntity.submissionId)))

    assertResult(StatusCheckComplete(true)) {
      await(monitor.handleStatusResponses(ExecutionServiceStatusResponse(workflowsRecs.map(r => scala.util.Success(Option((r.copy(status = status.toString), Option(ExecutionServiceOutputs(r.externalId.get, Map("o1" -> AttributeString("result")))))))))))
    }

    assertResult(Seq(testData.indiv1.copy(attributes = testData.indiv1.attributes + ("foo" -> AttributeString("result"))))) {
      testData.submissionUpdateEntity.workflows.map { wf =>
        runAndWait(entityQuery.get(SlickWorkspaceContext(testData.workspace), wf.workflowEntity.entityType, wf.workflowEntity.entityName)).get
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when workflow is done - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      val monitorRef = createSubmissionMonitorActor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))
      watch(monitorRef)
      expectMsgClass(5 seconds, classOf[Terminated])

      assertResult(SubmissionStatuses.Done) {
        runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateEntity.submissionId)).get.status
      }
    }
  }

  WorkflowStatuses.terminalStatuses.foreach { status =>
    it should s"terminate when workflow is aborted - $status" in withDefaultTestDatabase { dataSource: SlickDataSource =>
      runAndWait(submissionQuery.findById(UUID.fromString(testData.submissionUpdateEntity.submissionId)).map(_.status).update(SubmissionStatuses.Aborting.toString))
      val monitorRef = createSubmissionMonitorActor(dataSource, testData.submissionUpdateEntity, new SubmissionTestExecutionServiceDAO(status.toString))
      watch(monitorRef)
      expectMsgClass(5 seconds, classOf[Terminated])

      assertResult(SubmissionStatuses.Aborted) {
        runAndWait(submissionQuery.get(SlickWorkspaceContext(testData.workspace), testData.submissionUpdateEntity.submissionId)).get.status
      }
    }
  }

  def createSubmissionMonitorActor(dataSource: SlickDataSource, submission: Submission, execSvcDAO: ExecutionServiceDAO): TestActorRef[SubmissionMonitorActor] = {
    TestActorRef[SubmissionMonitorActor](SubmissionMonitorActor.props(
      testData.wsName,
      UUID.fromString(submission.submissionId),
      dataSource,
      execSvcDAO,
      new Builder().build(),
      1 millisecond
    ))
  }

  def createSubmissionMonitor(dataSource: SlickDataSource, submission: Submission, execSvcDAO: ExecutionServiceDAO): SubmissionMonitor = {
    new TestSubmissionMonitor(
      testData.wsName,
      UUID.fromString(submission.submissionId),
      dataSource,
      execSvcDAO,
      new Builder().build(),
      1 minutes
    )
  }

  private def ignoreStatusLastChangedDate(response: ExecutionServiceStatusResponse): ExecutionServiceStatusResponse = {
    ExecutionServiceStatusResponse(response.statusResponse.map {
      case scala.util.Success(Some((workflowRec, outputs))) => scala.util.Success(Some(workflowRec.copy(statusLastChangedDate = null), outputs))
      case otherwise => otherwise
    })
  }
}

class SubmissionTestExecutionServiceDAO(workflowStatus: => String) extends ExecutionServiceDAO {
  val abortedMap: scala.collection.concurrent.TrieMap[String, String] = new scala.collection.concurrent.TrieMap[String, String]()

  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo) = Future.successful(ExecutionServiceStatus("test_id", workflowStatus))
  override def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo) = Future.successful(Seq(Left(ExecutionServiceStatus("test_id", workflowStatus))))

  override def outputs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceOutputs(id, Map("o1" -> AttributeString("foo"))))
  override def logs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceLogs(id, Map("task1" -> Seq(ExecutionServiceCallLogs(stdout = "foo", stderr = "bar")))))

  override def status(id: String, userInfo: UserInfo) = {
    if(abortedMap.keySet.contains(id)) Future(ExecutionServiceStatus(id, WorkflowStatuses.Aborted.toString))
    else Future(ExecutionServiceStatus(id, workflowStatus))
  }
  override def abort(id: String, userInfo: UserInfo) = {
    abortedMap += id -> WorkflowStatuses.Aborting.toString
    Future.successful(Success(ExecutionServiceStatus(id, WorkflowStatuses.Aborting.toString)))
  }
  override def callLevelMetadata(id: String, userInfo: UserInfo) = Future.successful(null)
}

class TestSubmissionMonitor(val workspaceName: WorkspaceName,
                            val submissionId: UUID,
                            val datasource: SlickDataSource,
                            val executionServiceDAO: ExecutionServiceDAO,
                            val credential: Credential,
                            val submissionPollInterval: Duration) extends SubmissionMonitor